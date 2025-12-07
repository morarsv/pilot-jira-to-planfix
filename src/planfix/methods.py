import httpx
import base64
from lxml import etree
from pathlib import Path
from jinja2 import Template


def _to_cdata(text: str) -> str:
    # CDATA не может содержать "]]>" — разбиваем такими «швами»
    return "<![CDATA[" + text.replace("]]>", "]]]]><![CDATA[>") + "]]>"


async def get_sid(account: str,
                  login: str,
                  password: str,
                  api_key: str,
                  url: str) -> str:
    headers = {
        "Accept": "application/xml"
    }

    tmpl = Template("""    
    <request method="auth.login">          
        <account>{{ account }}</account>      
        <login>{{ login }}</login>      
        <password>{{ password }}</password>    
    </request>      
    """)

    data: str = tmpl.render(
        account=account,
        login=login,
        password=password
    )
    async with httpx.AsyncClient() as client:
        r = await client.post(
            url,
            auth=(api_key, ""),
            headers=headers,
            data=data
        )
        if r.status_code != 200:
            raise RuntimeError(f"soap fault: {r.status_code} {r.text}")

    try:
        root = etree.fromstring(r.content)
    except etree.XMLSyntaxError as e:
        raise RuntimeError(f"Невалидный xml: {e}: {r.text[:200]}")

    sid = root.xpath("string(//sid)") or None
    if not sid:
        raise RuntimeError("В ответе нет <sid>: " + r.text[:200])

    return sid


async def add_task(account: str,
                   api_key: str,
                   url: str,
                   sid: str,
                   workers_id: str,
                   members_id: str,
                   title: str,
                   description: str,
                   project_id: int,
                   jira_issue_link: str) -> str:
    headers = {
        "Accept": "application/xml"
    }
    title      = f'[JIRA] {title}'
    workers_id = workers_id.split(',')
    members_id = members_id.split(',')

    tmpl_user_id = Template("""
			        <id>{{ user_id }}</id>            
    """)

    members_tmpl_list: list = []
    for m in members_id:
        tmp = tmpl_user_id.render(
            user_id=m,
        )
        members_tmpl_list.append(tmp)

    workers_tmpl_list: list = []
    for w in workers_id:
        tmp = tmpl_user_id.render(
            user_id=w,
        )
        workers_tmpl_list.append(tmp)

    workers_id = '\n'.join(workers_tmpl_list)
    members_id = '\n'.join(members_tmpl_list)

    tmpl = Template("""  
    <request method="task.add">    
	    <account>{{ account }}</account>  
	    <sid>{{ sid }}</sid>    
	    <task>
		    <title>{{ title }} </title>          
	        <description>{{ description }}</description>          
	        <workers>              
                <users>   
                    {{ workers_id }}
                </users>          
	        </workers>  
	        <members>
                <users>            
                    {{ members_id }}       
                </users>
			</members>        
			<project>            
				<id>{{ project_id }}</id>        
			</project>    
		</task>    
    </request>    
    """)

    description = _to_cdata(description)
    description = description +'\n\n'+'Ссылка на задачу: '+ jira_issue_link


    data: str = tmpl.render(
        account=account,
        sid=sid,
        workers_id=workers_id,
        members_id=members_id,
        project_id=project_id,
        title=title,
        description=description
    )
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                url,
                auth=(api_key, ""),
                headers=headers,
                data=data
            )
            if r.status_code != 200:
                raise RuntimeError(f"soap fault: {r.status_code} {r.text}")
    except httpx.HTTPError as e:
        raise RuntimeError(f"soap fault: {e}")

    try:
        root = etree.fromstring(r.content)
    except etree.XMLSyntaxError as e:
        raise RuntimeError(f"Невалидный xml: {e}: {r.text[:200]}")

    general = root.xpath("string(//id)") or None
    if not general:
        raise RuntimeError("В ответе нет <general>: " + r.text[:200])

    return general


async def update_description_task(
        account: str,
        api_key: str,
        url: str,
        sid: str,
        issue_id: str,
        description: str,
        jira_issue_link: str) -> None:
    headers = {
        "Accept": "application/xml"
    }
    tmpl = Template("""  
        <request method="task.update">    
    	    <account>{{ account }}</account>  
    	    <sid>{{ sid }}</sid>    
    	    <task>
    	        <general>{{ issue_id }}</general>    
    	        <description>{{ description }}</description>          
    		</task>    
        </request>    
    """)

    description = _to_cdata(description)
    description = description + '\n\n' + 'Ссылка на задачу: ' + jira_issue_link

    data: str = tmpl.render(
        account=account,
        issue_id=issue_id,
        sid=sid,
        description=description
    )
    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                url,
                auth=(api_key, ""),
                headers=headers,
                data=data
            )
            if r.status_code != 200:
                raise RuntimeError(f"soap fault: {r.status_code} {r.text}")
    except httpx.HTTPError as e:
        raise RuntimeError(f"soap fault: {e}: {r.text[:200]}")


def b64(path: str) -> str:
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("ascii")


async def upload_file(account: str,
                      api_key: str,
                      url: str,
                      sid: str,
                      planfix_task_id: int,
                      jira_task_id: int) -> None:
    file = Template("""
               <file>          
                   <name>{{FILE_NAME}}</name>          
                   <sourceType>FILESYSTEM</sourceType>          
                   <body>{{FILE_BODY_B64}}</body>          
                   <newversion>1</newversion>        
                </file>
    """)
    out_dir = Path("downloads") / str(jira_task_id)
    if not out_dir.exists():
        raise FileNotFoundError(f"Directory {out_dir} does not exist.")
    f_names_list = [p.name for p in out_dir.iterdir() if p.is_file()]
    template_files_list: list = []
    for f in f_names_list:

        path = out_dir / f
        file_body_b64 = b64(path=str(path))
        tmp = file.render(
            FILE_NAME=f,
            FILE_BODY_B64=file_body_b64,
        )
        template_files_list.append(tmp)

    files = '\n'.join(template_files_list)

    xml = Template("""  
            <request method="file.upload">
              <account>{{ACCOUNT}}</account>      
              <sid>{{SID}}</sid>          
              <task><id>{{TASK_ID}}</id></task>
              <target>task</target>
                <files>
                   {{FILES}}   
                </files>    
            </request>
	""")

    data = xml.render(
        ACCOUNT=account,
        SID=sid,
        TASK_ID=planfix_task_id,
        FILES=files
    )

    headers = {"Accept": "application/xml",
               "Content-Type": "application/xml; charset=utf-8"}

    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                url,
                auth=(api_key, ""),
                headers=headers,
                data=data
            )
            if r.status_code != 200:
                raise RuntimeError(f"soap fault: {r.status_code} {r.text}")
    except Exception as e:
        raise RuntimeError(f"soap fault: {e}: {r.text[:200]}")


async def add_comment(account: str,
                      api_key: str,
                      url: str,
                      sid: str,
                      description: str,
                      planfix_task_id: int | str,
                      owner_id: str) -> str:

    xml = Template("""  
            <request method="action.add">
              <account>{{account}}</account>      
              <sid>{{sid}}</sid>          
              <action>
                <description>{{description}}</description>
                <task>
                  <id>{{task_id}}</id>
                </task>
                <owner>
                  <id>{{owner_id}}</id>
                </owner>   
              </action>
            </request>
	""")
    description = _to_cdata(description)

    data = xml.render(
        account=account,
        sid=sid,
        description=description,
        task_id=planfix_task_id,
        owner_id=owner_id
    )
    headers = {"Accept": "application/xml",
               "Content-Type": "application/xml; charset=utf-8"}

    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                url,
                auth=(api_key, ""),
                headers=headers,
                data=data
            )
            if r.status_code != 200:
                raise RuntimeError(f"soap fault: {r.status_code} {r.text}")
    except Exception as e:
        raise RuntimeError(f"soap fault: {e}: {r.text[:200]}")
    try:
        root = etree.fromstring(r.content)
    except etree.XMLSyntaxError as e:
        raise RuntimeError(f"Невалидный xml: {e}: {r.text[:200]}")

    id = root.xpath("string(//id)") or None
    if not id:
        raise RuntimeError("В ответе нет <id>: " + r.text[:200])

    return id


async def update_comment(account: str,
                         api_key: str,
                         url: str,
                         sid: str,
                         description: str,
                         comment_id: int | str) -> None:

    xml = Template("""  
            <request method="action.update">
              <account>{{account}}</account>      
              <sid>{{sid}}</sid>          
              <action>
                <id>{{comment_id}}</id>
                <description>{{description}}</description>   
              </action>
            </request>
	""")
    description = _to_cdata(description)

    data = xml.render(
        account=account,
        sid=sid,
        description=description,
        comment_id=comment_id,
    )

    headers = {"Accept": "application/xml",
               "Content-Type": "application/xml; charset=utf-8"}

    try:
        async with httpx.AsyncClient() as client:
            r = await client.post(
                url,
                auth=(api_key, ""),
                headers=headers,
                data=data
            )
            if r.status_code != 200:
                raise RuntimeError(f"soap fault: {r.status_code} {r.text}")
    except Exception as e:
        raise RuntimeError(f"soap fault: {e}: {r.text[:200]}")

    try:
        root = etree.fromstring(r.content)
    except etree.XMLSyntaxError as e:
        raise RuntimeError(f"Невалидный xml: {e}: {r.text[:200]}")

    if not root.xpath("string(//id)") or None:
        raise RuntimeError("В ответе нет <id>: " + str(r.content))
