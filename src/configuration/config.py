import os
from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env",
                                      env_file_encoding="utf-8",
                                      extra="ignore")

    BOT_TOKEN: str
    BOT_CHAT_ID: str

    REDIS_HOST: str
    REDIS_PORT: int
    JIRA_TOKEN: str
    JIRA_USERNAME: str
    JIRA_URL_ATTACHMENT_ISSUES: str
    JIRA_URL_SEARCH_ISSUES: str
    JIRA_URL_GET_COMMENTS: str
    JIRA_URL_ISSUE_LINK: str

    PLANFIX_API_KEY: str
    PLANFIX_URL: str
    PLANFIX_ACCOUNT: str
    PLANFIX_LOGIN: str
    PLANFIX_PASSWORD: str
    PLANFIX_MEMBERS: str
    PLANFIX_WORKERS: str
    PLANFIX_OWNER_COMMENT: str
    PLANFIX_PROJECT_ID: int

    SLEEP_INTERVAL: int = 14400
