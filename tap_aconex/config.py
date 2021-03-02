from tap_aconex.fetch import handle_projects


ID_FIELDS = {
    "projects": "ProjectId",
    "documents": "DocumentId",
    "mail": "MailId",
}


SYNC_FUNCTIONS = {
    "projects": handle_projects,
}

SUB_STREAMS = {
    "projects": [
        "mail",
        "documents",
    ],
}
