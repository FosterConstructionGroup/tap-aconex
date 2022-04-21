from tap_aconex.fetch import handle_projects


ID_FIELDS = {
    "documents": "DocumentId",
    "mail": "MailId",
    "projects": "ProjectId",
    "organisations": "OrganizationId",
}


SYNC_FUNCTIONS = {
    "projects": handle_projects,
}

SUB_STREAMS = {
    "projects": ["documents", "mail", "organisations"],
}
