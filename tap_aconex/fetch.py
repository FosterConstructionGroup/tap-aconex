from datetime import datetime, timezone, timedelta
import re
import singer
from singer import metadata
from singer.bookmarks import get_bookmark
from tap_aconex.utility import (
    get_generic,
    get_all_pages,
    datetime_format,
    parse_date,
    format_date,
    date_format,
    coerce_to_list,
)


def handle_projects(resource, schemas, state, mdata):
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()
    sync_organisations = "organisations" in schemas
    # store in a dict to deduplicate results (faster than Redshift doing so)
    organisations = {}

    res = get_generic(resource, "projects")
    rows = res["ProjectResults"]["SearchResults"]["Project"]

    for row in rows:
        row["Active"] = row["@Active"]
        write_record(row, resource, schemas[resource], mdata, extraction_time)

        # don't try to fetch documents or mail for inactive projects as the API will error
        if row["Active"] == "false":
            continue

        if "documents" in schemas:
            handle_documents(row["ProjectId"], schemas["documents"], state, mdata)
        if "mail" in schemas:
            mail_schemas = {"mail": schemas["mail"]}
            if "organisations" in schemas:
                mail_schemas["organisations"] = schemas["organisations"]
            orgs = handle_mail(
                row["ProjectId"], mail_schemas, state, mdata, sync_organisations
            )
            for (id, name) in orgs:
                organisations[id] = name

    if sync_organisations:
        organisations_rows = [
            {"OrganizationId": k, "OrganizationName": v}
            for (k, v) in organisations.items()
        ]
        write_many(
            organisations_rows,
            "organisations",
            schemas["organisations"],
            mdata,
            extraction_time,
        )

    if "documents" in schemas:
        state = write_bookmark(state, "documents", extraction_time)
    if "mail" in schemas:
        state = write_bookmark(state, "mail", extraction_time)
        if "organisations" in schemas:
            state = write_bookmark(state, "organisations", extraction_time)

    return write_bookmark(state, resource, extraction_time)


def handle_documents(project_id, schema, state, mdata):
    resource = "documents"
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    fields = "&return_fields=approved,author,contractnumber,date1,docno,doctype,filename,fileSize,fileType,modifiedby,numberOfMarkups,packagenumber,received,reference,registered,reviewed,revision,scale,statusid,tagNumber,title,toclient,trackingid,versionnumber"
    filter = get_filter_string(bookmark, extraction_time, "registered")

    rows = get_all_pages(
        resource,
        f"projects/{project_id}/register",
        "Document",
        "RegisterSearch",
        extra_query_string=f"&{fields}&{filter}",
    )

    for r in rows:
        r["ProjectId"] = project_id
        r["DocumentId"] = r["@DocumentId"]

    write_many(rows, resource, schema, mdata, extraction_time)


def handle_mail(project_id, schemas, state, mdata, sync_organisations):
    resource = "mail"
    schema = schemas[resource]
    bookmark = get_bookmark(state, resource, "since")
    # Current time in local timezone as "aware datetime", per https://stackoverflow.com/a/25887393/7170445
    extraction_time = datetime.now(timezone.utc).astimezone()

    fields = "&return_fields=attribute,closedoutdetails,confidential,corrtypeid,docno,fromUserDetails,inreftomailno,mailRecipients,reasonforissueid,responsedate,secondaryattribute,sentdate,subject,tostatusid,totalAttachmentsSize,attachedDocumentCount"
    filter = get_filter_string(bookmark, extraction_time, "sentdate")

    rows = get_all_pages(
        resource,
        f"projects/{project_id}/mail",
        "Mail",
        "MailSearch",
        extra_query_string=f"&mail_box=sentbox{fields}{filter}",
    )

    organisations = []
    if sync_organisations:
        organisations = [
            (r["OrganizationId"], r["OrganizationName"])
            for row in rows
            for r in coerce_to_list(row["ToUsers"]["Recipient"])
        ]

    for r in rows:
        r["ProjectId"] = project_id
        r["MailId"] = r["@MailId"]
        try:
            r["sent_to"] = next(
                r["OrganizationName"]
                for r in coerce_to_list(r["ToUsers"]["Recipient"])
                if r["DistributionType"] == "TO"
            )
        except:
            pass

    write_many(rows, resource, schema, mdata, extraction_time)
    return organisations


def get_filter_string(bookmark, extraction_time, field):
    if bookmark is None:
        return ""
    else:
        filter_date_format = "%Y%m%d"
        filter_start = format_date(
            parse_date(bookmark, datetime_format) - timedelta(days=2),
            filter_date_format,
        )
        filter_end = format_date(
            extraction_time + timedelta(days=2), filter_date_format
        )
        return f"&search_query={field}:[{filter_start} TO {filter_end}]"


def write_many(rows, resource, schema, mdata, dt):
    for row in rows:
        write_record(row, resource, schema, mdata, dt)


def write_record(row, resource, schema, mdata, dt):
    with singer.Transformer() as transformer:
        rec = transformer.transform(row, schema, metadata=metadata.to_map(mdata))
    singer.write_record(resource, rec, time_extracted=dt)


def write_bookmark(state, resource, dt):
    singer.write_bookmark(state, resource, "since", format_date(dt))
    return state
