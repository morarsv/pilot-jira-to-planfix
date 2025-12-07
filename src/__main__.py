import asyncio
import logging
import pytz
import os
import sys

from asyncio import CancelledError
from redis import Redis
from redis.exceptions import ConnectionError
from src.utils.utils import current_time, CustomFormatter
from src.utils.alert_tg import send_alert_to_chat
from src.jira import methods as jira
from src.planfix import methods as planfix
from src.storage import r_storage as storage, repository as repo
from src.utils import hash as h
from src.configuration.config import Settings


logger = logging.getLogger(__name__)


async def hash_jira_issue_data(data: list) -> list[dict[str, str | None]]:
    """
    Возвращаем list[dict] хэш полей, для сравнения: issue_id, h_description, h_attachment.
    """
    issue_hash_list: list[dict[str, str | None]] = []
    for issue in data:
        issue_hash = dict()
        issue_hash['issue_id'] = issue['id']
        text = h.canon_text(s=issue['description'])
        issue_hash['h_description'] = h.hash_text(text=text)
        if issue['attachmentCount']:
            attachment = [a['id'] for a in issue['attachment']]
            issue_hash['h_attachment'] = h.hash_attachment_id(attachment)
        else:
            issue_hash['h_attachment'] = None

        issue_hash_list.append(issue_hash)
    return issue_hash_list


def comment_description_format(data: dict) -> str:
    """
    Возвращает форматированный текст комментария.
    """
    return (f'Автор: {data["author"]}.\n\n'
            f'Дата: {data["created"][:19]}.\n\n'
            f'{data['description']}')


def logging_config() -> None:
    """
    Настройка логирования.
    """
    _log_directory = 'logs/'
    time = current_time()
    timezone = pytz.timezone("Asia/Novosibirsk")
    log_file_path = os.path.join(_log_directory, f'{time[:10]}_err_generator.log')
    os.makedirs(os.path.dirname(log_file_path), exist_ok=True)
    file_handler = logging.FileHandler(filename=log_file_path, mode='a', encoding='utf-8')
    log_format = '[{asctime}] #{levelname:<8} {filename} - {lineno} - {name} - {message}'
    date_format = "%Y-%m-%d %H:%M:%S"
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        style='{',
        handlers=[
            logging.StreamHandler()
        ]
    )
    file_handler.setLevel(logging.ERROR)
    file_handler.setFormatter(
        CustomFormatter(fmt=log_format, datefmt=date_format, style='{', tz=timezone)
    )
    logging.getLogger().addHandler(file_handler)


async def comments(conf: Settings,
                   jira_issues_data: list,
                   r: Redis,
                   sid: str) -> None:
    """
    Работа с комментариями.
    Если есть комментарии в задачах которые отслеживаются, загружаем комментарии из Jira.
    Если комментарий отсутствует в Redis, то он отсутствует и в Planfix, по API Planfix добавляем его к указанной задаче,
    и записываем в Redis: comment_jira_id, comment_planfix_id, planfix_issue_id, h_description.
    Если комментарий присутствует в Redis, мы сравниваем хэш значения description текущего комментария с h_description из
    Redis. Если хэш разный, то обновляем комментарий в Planfix и обновляем хэш значение в Redis. Если хэш равен,
    то пропускаем.
    """
    request = [jira.get_issue_comments(issue_id=i['id']) for i in jira_issues_data]
    comments_data = await asyncio.gather(*request)

    if comments_data:
        for comment_list in comments_data:

            if comment_list:
                for data in comment_list:
                    redis_comment = await repo.get_comment(r=r, comment_j_id=data['id'])

                    if redis_comment:
                        """Сравниваем хэш, отслеживаем изменения"""
                        description = comment_description_format(data=data)
                        text = h.canon_text(s=description)
                        h_description = h.hash_text(text=text)
                        redis_h_description = redis_comment['h_description']
                        equal = h.hashes_equal(h1=redis_h_description,
                                               h2=h_description)
                        if not equal:

                            try:
                                await planfix.update_comment(
                                    account=conf.PLANFIX_ACCOUNT,
                                    api_key=conf.PLANFIX_API_KEY,
                                    url=conf.PLANFIX_URL,
                                    sid=sid,
                                    description=description,
                                    comment_id=redis_comment['comment_p_id'],
                                )
                                await repo.upsert_comment(
                                    r=r,
                                    comment_j_id=data['id'],
                                    comment_p_id=redis_comment['comment_p_id'],
                                    p_issue_id=redis_comment['p_issue_id'],
                                    h_description=h_description
                                )
                                logger.info(f'Комментарий был успешно обновлен в Planfix и redis. '
                                            f'Planfix comment ID: {redis_comment["comment_p_id"]} '
                                            f'Jira comment ID: {redis_comment["comment_j_id"]}')
                            except RuntimeError:
                                logger.error('Что то пошло не так, в блоке работы с обновлением комментариев')
                                await send_alert_to_chat(text=f'Что то пошло не так, '
                                                              f'в блоке работы с новыми комментариев'
                                                              f'Jira ID: {data['issue_id']}',
                                                         chat_id=conf.BOT_CHAT_ID)

                    else:
                        """Комментарий отсутствует в Redis, добавляем его в Planfix и записываем в Redis"""
                        jira_planfix_link = await repo.get_issue_link(r=r, j_issue=data['issue_id'])

                        if not jira_planfix_link:
                            logger.info(f'Связь Jira + Planfix отсутствует в redis, необходимо проверить. '
                                        f'Jira ID: {data['issue_id']}')
                            await send_alert_to_chat(text=f'Связь Jira + Planfix отсутствует в redis, '
                                                          f'необходимо проверить. '
                                                          f'Jira ID: {data['issue_id']}',
                                                     chat_id=conf.BOT_CHAT_ID)
                            continue

                        description = comment_description_format(data=data)
                        try:
                            comment_p_id = await planfix.add_comment(
                                account=conf.PLANFIX_ACCOUNT,
                                api_key=conf.PLANFIX_API_KEY,
                                url=conf.PLANFIX_URL,
                                sid=sid,
                                description=description,
                                planfix_task_id=jira_planfix_link['p_issue'],
                                owner_id=conf.PLANFIX_OWNER_COMMENT
                            )

                            text = h.canon_text(s=description)
                            h_description = h.hash_text(text=text)

                            await repo.upsert_comment(
                                r=r,
                                comment_j_id=data['id'],
                                comment_p_id=comment_p_id,
                                p_issue_id=jira_planfix_link['p_issue'],
                                h_description=h_description
                            )
                            logger.info(
                                f'Новый комментарий добавлен к задаче. Planfix task ID: {jira_planfix_link['p_issue']}')
                        except Exception as ex:
                            logger.error(f'Что то пошло не так, в блоке работы с новыми комментариями.\n'
                                         f'Error: {ex}')
                            await send_alert_to_chat(text=f'Что то пошло не так, '
                                                          f'в блоке работы с новыми комментариями'
                                                          f'Jira ID: {data['issue_id']}',
                                                     chat_id=conf.BOT_CHAT_ID)


async def in_redis_issues(in_redis_issues_ids: list,
                          r: Redis,
                          issue_hash_list: list,
                          updated_issues_data: list,
                          upload_issues_to_planfix_ids: list,
                          conf: Settings) -> None:
    """
    Если задачи присутствуют в Redis, мы сравниваем хэш description, attachment текущей задачи с данными из Redis.
    Если хэш равный, то пропускаем задачу.
    Если хэш разный, то добавляем в updated_issues_data данные об изменениях(id: int, h_description: bool,
    h_attachment: bool). В upload_issues_to_planfix_ids добавляем jira_issue_id.
    """
    if not in_redis_issues_ids:
        return

    for issue_id in in_redis_issues_ids:
        try:

            equal: bool
            redis_issue = await repo.get_issue(r=r, issue_id=issue_id)
            jira_issue = next(i for i in issue_hash_list if int(i['issue_id']) == issue_id)

            equal_description: bool = h.hashes_equal(h1=redis_issue['h_description'],
                                                     h2=jira_issue['h_description'])
            if redis_issue.get('h_attachment') and jira_issue.get('h_attachment'):
                equal_attachment: bool = h.hashes_equal(h1=redis_issue['h_attachment'],
                                                        h2=jira_issue['h_attachment'])
            elif not redis_issue.get('h_attachment') and not jira_issue.get('h_attachment'):
                equal_attachment = True
            else:
                equal_attachment = False

            equal = False if equal_description and equal_attachment else True

            if equal:
                issue_data: dict = dict()
                issue_data['id'] = issue_id
                issue_data['h_description'] = not equal_description
                issue_data['h_attachment'] = not equal_attachment
                updated_issues_data.append(issue_data)
                upload_issues_to_planfix_ids.append(issue_id)
        except Exception as ex:
            logger.error(f'Ошибка при сравнении хэша. Ошибка: {ex}')
            await send_alert_to_chat(text=f'Ошибка при сравнении хэша. Ошибка: {ex}',
                                     chat_id=conf.BOT_CHAT_ID)


async def upload_issues_to_planfix(upload_issues_to_planfix_ids: list,
                                   issue_hash_list: list,
                                   updated_issues_data: list,
                                   jira_issues_list_data: list,
                                   r: Redis,
                                   conf: Settings,
                                   sid: str) -> None:
    """
    Если upload_issues_to_planfix_ids пуст, то return.
    Записываем/обновляем данные о задачах в Redis.
    Записываем связь Jira+Planfix по новым задачам в Redis.
    Создаем/обновляем задачи в Planfix через API.
    """
    if not upload_issues_to_planfix_ids:
        return

    for issue_id in issue_hash_list:
        if int(issue_id['issue_id']) in upload_issues_to_planfix_ids:
            try:

                result = await repo.upsert_issue_hash(r=r,
                                                      issue_id=int(issue_id['issue_id']),
                                                      h_description=issue_id['h_description'],
                                                      h_attachment=issue_id['h_attachment'])
                if result:
                    logger.info(f'Новая запись добавлена. Jira ID: {issue_id['issue_id']}')
                else:
                    logger.info(f'Запись обновлена. Jira ID: {issue_id['issue_id']}')
            except Exception as ex:
                logger.error(f'Ошибка при записи/обновлении задачи jira в redis. '
                             f'Jira ID: {issue_id['issue_id']}. '
                             f'Ошибка: {ex}')
                await send_alert_to_chat(text=f'Ошибка при записи/обновлении задачи jira в redis. '
                                              f'Jira ID: {issue_id['issue_id']}. '
                                              f'Ошибка: {ex}',
                                         chat_id=conf.BOT_CHAT_ID)

    in_planfix_ids: list[tuple] = []
    not_in_planfix_ids: list[int] = []

    for issue_id in upload_issues_to_planfix_ids:
        jira_planfix = await repo.get_issue_link(r=r,
                                                 j_issue=issue_id)
        if jira_planfix:
            in_planfix_ids.append((int(jira_planfix['p_issue']), issue_id))
        else:
            not_in_planfix_ids.append(issue_id)

    if in_planfix_ids:
        for issue_id in in_planfix_ids:

            issue_data = next(i for i in updated_issues_data if i['id'] == issue_id[1])
            jira_issue_data = next(i for i in jira_issues_list_data if int(i['id']) == issue_id[1])

            try:
                if issue_data.get('h_description'):
                    await planfix.update_description_task(
                        account=conf.PLANFIX_ACCOUNT,
                        api_key=conf.PLANFIX_API_KEY,
                        url=conf.PLANFIX_URL,
                        sid=sid,
                        issue_id=issue_id[0],
                        description=jira_issue_data['description'],
                        jira_issue_link=jira_issue_data.get('issue_link', ''),
                    )
            except RuntimeError:
                logger.error(f'Ошибка при обновлении описании задачи в planfix. '
                             f'Planfix ID: {issue_id[0]}. Jira ID: {issue_id[1]}')
                await send_alert_to_chat(text=f'Ошибка при обновлении описании задачи в planfix. '
                                              f'Planfix ID: {issue_id[0]}. Jira ID: {issue_id[1]}',
                                         chat_id=conf.BOT_CHAT_ID)

            try:
                if issue_data.get('h_attachment'):
                    list_saved_attachments = await jira.get_issue_attachments(
                        attachments=jira_issue_data['attachment'],
                        issue_key=issue_id[0]
                    )
                    logger.info(f'Вложения загружены. Задача ID: {issue_id} '
                                f'Путь: {list_saved_attachments}')

                    await planfix.upload_file(
                        account=conf.PLANFIX_ACCOUNT,
                        api_key=conf.PLANFIX_API_KEY,
                        url=conf.PLANFIX_URL,
                        sid=sid,
                        planfix_task_id=issue_id[0],
                        jira_task_id=issue_id[1]
                    )
            except RuntimeError:
                logger.error(f'Не удалось обновить вложения в planfix. '
                             f'Planfix ID: {issue_id[0]}, Jira ID: {issue_id[1]}')
                await send_alert_to_chat(text=f'Не удалось обновить вложения в planfix. '
                                              f'Planfix ID: {issue_id[0]}, Jira ID: {issue_id[1]}',
                                         chat_id=conf.BOT_CHAT_ID)

    if not_in_planfix_ids:

        jira_planfix_ids: list[tuple[int, int, int]] = []

        for issue_id in not_in_planfix_ids:

            jira_issue_data = next(i for i in jira_issues_list_data if int(i['id']) == issue_id)
            try:
                p_issue_id = await planfix.add_task(
                    account=conf.PLANFIX_ACCOUNT,
                    api_key=conf.PLANFIX_API_KEY,
                    url=conf.PLANFIX_URL,
                    sid=sid,
                    workers_id=conf.PLANFIX_WORKERS,
                    members_id=conf.PLANFIX_MEMBERS,
                    title=jira_issue_data.get('title'),
                    description=jira_issue_data.get('description'),
                    project_id=conf.PLANFIX_PROJECT_ID,
                    jira_issue_link=jira_issue_data.get('issue_link', ''),
                )
            except RuntimeError:
                logger.error(f'Ошибка при создании задачи в planfix. '
                             f'Jira ID: {jira_issue_data["id"]}')
                await send_alert_to_chat(text=f'Ошибка при создании задачи в planfix. '
                                              f'Jira ID: {jira_issue_data["id"]}',
                                         chat_id=conf.BOT_CHAT_ID)
                return

            jira_planfix_ids.append((issue_id, int(p_issue_id), jira_issue_data['attachmentCount']))
            await repo.upsert_issue_link(r=r,
                                         j_issue=issue_id,
                                         p_issue=int(p_issue_id))

            try:
                if jira_issue_data['attachment']:
                    list_saved_attachments = await jira.get_issue_attachments(
                        attachments=jira_issue_data['attachment'],
                        issue_key=issue_id
                    )
                    logger.info(f'Вложения загружены. Задача ID: {issue_id} '
                                f'Путь: {list_saved_attachments}')
                    await planfix.upload_file(
                        account=conf.PLANFIX_ACCOUNT,
                        api_key=conf.PLANFIX_API_KEY,
                        url=conf.PLANFIX_URL,
                        sid=sid,
                        planfix_task_id=int(p_issue_id),
                        jira_task_id=issue_id
                    )
            except RuntimeError:
                logger.error(f'Ошибка при загрузки вложений. '
                             f'Planfix ID: {p_issue_id}')
                await send_alert_to_chat(text=f'Ошибка при загрузки вложений.\n'
                                              f'Planfix ID: {p_issue_id}',
                                         chat_id=conf.BOT_CHAT_ID)


async def job():

    conf = Settings()

    logging_config()

    try:
        jira_issues_self_data = await jira.get_issues_self()
        request = [jira.get_issue_data(url=u) for u in jira_issues_self_data]
        jira_issues_list_data = await asyncio.gather(*request)
    except Exception as ex:
        logger.error(f"Не удалось получить список задач Jira. Error: {ex}")
        await send_alert_to_chat(text='Не удалось получить список задач Jira.',
                                 chat_id=conf.BOT_CHAT_ID)
        sys.exit(1)


    issue_hash_list = await hash_jira_issue_data(data=jira_issues_list_data)
    jira_issues_ids = [int(i['issue_id']) for i in issue_hash_list]

    try:
        sid = await planfix.get_sid(
            account=conf.PLANFIX_ACCOUNT,
            login=conf.PLANFIX_LOGIN,
            password=conf.PLANFIX_PASSWORD,
            api_key=conf.PLANFIX_API_KEY,
            url=conf.PLANFIX_URL,
        )
        logger.info('SID Planfix успешно получен.')
    except Exception as ex:
        logger.exception(f"Не удалось получить SID Planfix. Error: {ex}")
        await send_alert_to_chat(text='Не удалось получить SID Planfix.',
                                 chat_id=conf.BOT_CHAT_ID)
        sys.exit(1)

    async with storage.redis_client(host=conf.REDIS_HOST,
                                    port=conf.REDIS_PORT,
                                    decode_responses=True) as r:
        redis_issues_ids = await repo.list_issue_ids(r=r, batch=500)

        updated_issues_data: list[dict] = [] # список измененных задач. dict = {id: int, description: bool, attachment: bool}

        upload_issues_to_planfix_ids = list(set(jira_issues_ids) - set(redis_issues_ids))
        in_redis_issues_ids = list(set(jira_issues_ids) - set(upload_issues_to_planfix_ids))

        await in_redis_issues(
            in_redis_issues_ids=in_redis_issues_ids,
            r=r,
            issue_hash_list=issue_hash_list,
            updated_issues_data=updated_issues_data,
            upload_issues_to_planfix_ids=upload_issues_to_planfix_ids,
            conf=conf)

        await upload_issues_to_planfix(
            upload_issues_to_planfix_ids=upload_issues_to_planfix_ids,
            issue_hash_list=issue_hash_list,
            updated_issues_data=updated_issues_data,
            jira_issues_list_data=jira_issues_list_data,
            r=r,
            conf=conf,
            sid=sid)

        await comments(
            conf=conf,
            jira_issues_data=jira_issues_list_data,
            r=r,
            sid=sid)

async def main():
    conf = Settings()
    while True:
        try:
            await job()
        except Exception as ex:
            logger.error(f"Error occurred while executing job: {ex}")
        finally:
            await asyncio.sleep(conf.SLEEP_INTERVAL)

if __name__ == '__main__':
    try:
        asyncio.run(main())
    except KeyboardInterrupt or CancelledError:
        logging.error("Shutting down...")
    except ConnectionError as e:
        logger.error(f'Redis exception connection error: {e}')
