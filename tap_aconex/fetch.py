from datetime import datetime, timezone
import re
import singer
import singer.metrics as metrics
from singer import metadata
from singer.bookmarks import get_bookmark
from tap_aconex.utility import (
    get_generic,
    get_all_pages,
    datetime_format,
    parse_date,
    format_date,
    date_format,
)


def handle_projects(resource, schemas, state, mdata):
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    res = get_generic(resource, "projects")
    rows = res["ProjectResults"]["SearchResults"]["Project"]

    write_many(rows, resource, schemas[resource], mdata, extraction_time)

    for row in rows:
        if "documents" in schemas:
            state = handle_documents(
                row["ProjectId"], schemas["documents"], state, mdata
            )
        if "mail" in schemas:
            state = handle_mail(row["ProjectId"], schemas["mail"], state, mdata)

    return write_bookmark(state, resource, extraction_time)


def handle_documents(project_id, schema, state, mdata):
    resource = "documents"
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    fields = "return_fields=approved,author,contractnumber,date1,docno,doctype,filename,fileSize,fileType,modifiedby,numberOfMarkups,packagenumber,received,reference,registered,reviewed,revision,scale,statusid,tagNumber,title,toclient,trackingid,versionnumber"
    filter = ""

    if bookmark is not None:
        filter_date_format = "%Y%m%d"
        filter_start = format_date(
            parse_date(bookmark, datetime_format), filter_date_format
        )
        filter_end = format_date(extraction_time, filter_date_format)
        filter = f"search_query=registered:[{filter_start} TO {filter_end}]"

    rows = get_all_pages(
        resource,
        f"projects/{project_id}/register",
        "Document",
        "RegisterSearch",
        extra_query_string=f"&{fields}&{filter}",
    )

    for r in rows:
        r["ProjectId"] = project_id

    write_many(rows, resource, schema, mdata, extraction_time)
    return write_bookmark(state, resource, extraction_time)


def handle_mail(project_id, schema, state, mdata):
    resource = "mail"
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    fields = "return_fields=attribute,closedoutdetails,confidential,corrtypeid,docno,fromUserDetails,inreftomailno,mailRecipients,reasonforissueid,responsedate,secondaryattribute,sentdate,subject,tostatusid,totalAttachmentsSize,attachedDocumentCount"
    filter = ""

    if bookmark is not None:
        filter_date_format = "%Y%m%d"
        filter_start = format_date(
            parse_date(bookmark, datetime_format), filter_date_format
        )
        filter_end = format_date(extraction_time, filter_date_format)
        filter = f"search_query=senddate:[{filter_start} TO {filter_end}]"

    rows = get_all_pages(
        resource,
        f"projects/{project_id}/mail",
        "Mail",
        "MailSearch",
        extra_query_string=f"&mail_box=sentbox&{fields}&{filter}",
    )

    for r in rows:
        r["ProjectId"] = project_id

    write_many(rows, resource, schema, mdata, extraction_time)
    return write_bookmark(state, resource, extraction_time)


def write_many(rows, resource, schema, mdata, dt):
    with metrics.record_counter(resource) as counter:
        for row in rows:
            write_record(row, resource, schema, mdata, dt)
            counter.increment()


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", format_date(dt))
    return state
