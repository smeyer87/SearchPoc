from avro import schema

MySchema = """
{"type": "record",
"name": "EDIClaims",
"fields": [
    {"name": "rowid", "type": "string"},
    {"name": "inputfile", "type": "string"},
    {"name": "claimheader400a",
        "type": {
                "type": "record",
                "name": "400a",
                "fields":
                  [
                    { "name": "recordtype", "type": "string"},
                    { "name": "record-aggregator", "type": "string"},
                    { "name": "record-type", "type": "string"},
                    { "name": "record-type-seq", "type": "string"},
                    { "name": "record-type-qual", "type": "string"},
                    { "name": "record-type-qual-seq", "type": "string"},
                    { "name": "header-sender-id", "type": "string"},
                    { "name": "prod-or-test", "type": "string"},
                    { "name": "group-submitter-id", "type": "string"},
                    { "name": "group-receiver-id", "type": "string"},
                    { "name": "hipaa-release", "type": "string"},
                    { "name": "received-date", "type": "string"},
                    { "name": "claim-or-encounter-ind", "type": "string"},
                    { "name": "service-category", "type": "string"},
                    { "name": "brand-ind", "type": "string"},
                    { "name": "claim-routed-to", "type": "string"},
                    { "name": "compliance-expected", "type": "string"},
                    { "name": "submitter-type", "type": "string"},
                    { "name": "submitter-id-qual", "type": "string"},
                    { "name": "submitter-id", "type": "string"},
                    { "name": "submitter-name-type", "type": "string"},
                    { "name": "submitter-name-last", "type": "string"},
                    { "name": "submitter-name-first", "type": "string"},
                    { "name": "submitter-name-middle", "type": "string"},
                    { "name": "receiver-receiver-id", "type": "string"},
                    { "name": "medicare-crossover-ind", "type": "string"},
                    { "name": "enterprise-edi-dcn", "type": "string"},
                    { "name": "local-system-dcn-1", "type": "string"},
                    { "name": "local-system-dcn-2", "type": "string"},
                    { "name": "sender-group-control", "type": "string"},
                    { "name": "filler", "type": "string"}
                  ]
                }
    },
        { "name": "submittercontactinfo400b",
        "type": {
                "type": "record",
                "name": "400b",
                "fields":
                  [
                    { "name": "recordtype", "type": "string"},
                    { "name": "record-aggregator", "type": "string"},
                    { "name": "record-type", "type": "string"},
                    { "name": "record-type-seq", "type": "string"},
                    { "name": "record-type-qual", "type": "string"},
                    { "name": "record-type-qual-seq", "type": "string"},
                    { "name": "submitr-contact-name", "type": "string"},
                    { "name": "submitr-contact-qual", "type": "string"},
                    { "name": "submitr-contact-number", "type": "string"},
                    { "name": "filler", "type": "string"}
                  ]
                }
    },
        {"name": "billingprovider401a",
        "type": {
                 "type": "record",
                 "name": "401a",
                 "fields":
                    [
                        { "name": "recordtype", "type": "string"},
                        { "name": "record-aggregator", "type": "string"},
                        { "name": "record-type", "type": "string"},
                        { "name": "record-type-seq", "type": "string"},
                        { "name": "record-type-qual", "type": "string"},
                        { "name": "record-type-qual-seq", "type": "string"},
                        { "name": "provider-type", "type": "string"},
                        { "name": "name-type", "type": "string"},
                        { "name": "name-last-or-org", "type": "string"},
                        { "name": "name-first", "type": "string"},
                        { "name": "name-middle", "type": "string"},
                        { "name": "name-suffix", "type": "string"},
                        { "name": "provider-id-qual", "type": "string"},
                        { "name": "provider-id", "type": "string"},
                        { "name": "street-address-1", "type": "string"},
                        { "name": "street-address-2", "type": "string"},
                        { "name": "city-name", "type": "string"},
                        { "name": "state-code", "type": "string"},
                        { "name": "zip-code", "type": "string"},
                        { "name": "country-code", "type": "string"},
                        { "name": "country-subdivision", "type": "string"},
                        { "name": "provider-other-id-tbl", "type": "string"},
                        { "name": "taxonomy-code-list-id", "type": "string"},
                        { "name": "taxonomy-code", "type": "string"},
                        { "name": "currency-code", "type": "string"},
                        { "name": "filler", "type": "string"}
                    ]
                 }
    },
        {"name": "billingprovidercontactinfo401b",
     "type": {
              "type": "record",
              "name": "401b",
              "fields":
                    [
                        { "name": "recordtype", "type": "string"},
                        { "name": "record-aggregator", "type": "string"},
                        { "name": "record-type", "type": "string"},
                        { "name": "record-type-seq", "type": "string"},
                        { "name": "record-type-qual", "type": "string"},
                        { "name": "record-type-qual-seq", "type": "string"},
                        { "name": "billing-contact-name", "type": "string"},
                        { "name": "billing-contact-qual", "type": "string"},
                        { "name": "billing-contact-number", "type": "string"},
                        { "name": "filler", "type": "string"}
                    ]
                }
    },
        {"name": "pay-toprovider402a",
     "type": {
               "type": "record",
               "name": "402a",
               "fields":
                    [
                        { "name": "recordtype", "type": "string"},
                        { "name": "record-aggregator", "type": "string"},
                        { "name": "record-type", "type": "string"},
                        { "name": "record-type-seq", "type": "string"},
                        { "name": "record-type-qual", "type": "string"},
                        { "name": "record-type-qual-seq", "type": "string"},
                        { "name": "provider-type", "type": "string"},
                        { "name": "name-type", "type": "string"},
                        { "name": "street-address-1", "type": "string"},
                        { "name": "street-address-2", "type": "string"},
                        { "name": "city-name", "type": "string"},
                        { "name": "state-code", "type": "string"},
                        { "name": "zip-code", "type": "string"},
                        { "name": "country-code", "type": "string"},
                        { "name": "country-subdivision", "type": "string"},
                        { "name": "filler", "type": "string"}
                    ]
                }
    },
        {"name": "pay-toplan410a",
     "type": {
              "type": "record",
              "name": "410a",
              "fields":
                    [
                        { "name": "recordtype", "type": "string"},
                        { "name": "record-aggregator", "type": "string"},
                        { "name": "record-type", "type": "string"},
                        { "name": "record-type-seq", "type": "string"},
                        { "name": "record-type-qual", "type": "string"},
                        { "name": "record-type-qual-seq", "type": "string"},
                        { "name": "pay-to-plan-qual", "type": "string"},
                        { "name": "name-type", "type": "string"},
                        { "name": "name-last-or-org", "type": "string"},
                        { "name": "plan-id-qual", "type": "string"},
                        { "name": "plan-id", "type": "string"},
                        { "name": "street-address-1", "type": "string"},
                        { "name": "street-address-2", "type": "string"},
                        { "name": "city-name", "type": "string"},
                        { "name": "state-code", "type": "string"},
                        { "name": "zip-code", "type": "string"},
                        { "name": "country-code", "type": "string"},
                        { "name": "country-subdivision", "type": "string"},
                        { "name": "plan-other-id-table", "type": "string"},
                        { "name": "filler", "type": "string"}
                    ]
                }
    },
        {"name": "subscriber421a",
     "type": {
             "type": "record",
             "name": "421a",
             "fields":
                    [
                        { "name": "recordtype", "type": "string"},
                        { "name": "record-aggregator", "type": "string"},
                        { "name": "record-type", "type": "string"},
                        { "name": "record-type-seq", "type": "string"},
                        { "name": "record-type-qual", "type": "string"},
                        { "name": "record-type-qual-seq", "type": "string"},
                        { "name": "subscriber-type", "type": "string"},
                        { "name": "payer-responsblty-seq", "type": "string"},
                        { "name": "relationship-code", "type": "string"},
                        { "name": "group-number", "type": "string"},
                        { "name": "group-name", "type": "string"},
                        { "name": "insurance-type", "type": "string"},
                        { "name": "claim-filing-indicator", "type": "string"},
                        { "name": "name-type", "type": "string"},
                        { "name": "name-last-or-org", "type": "string"},
                        { "name": "name-first", "type": "string"},
                        { "name": "name-middle", "type": "string"},
                        { "name": "name-suffix", "type": "string"},
                        { "name": "membership-id-qual", "type": "string"},
                        { "name": "membership-id", "type": "string"},
                        { "name": "street-address-1", "type": "string"},
                        { "name": "street-address-2", "type": "string"},
                        { "name": "city-name", "type": "string"},
                        { "name": "state-code", "type": "string"},
                        { "name": "zip-code", "type": "string"},
                        { "name": "country-code", "type": "string"},
                        { "name": "country-subdivision", "type": "string"},
                        { "name": "date-of-birth", "type": "string"},
                        { "name": "gender-code", "type": "string"},
                        { "name": "subscrbr-other-id-tbl", "type": "string"},
                        { "name": "its-relationship-code", "type": "string"},
                        { "name": "its-sec-payor-prc-ind", "type": "string"},
                        { "name": "filler", "type": "string"}
                    ]
                }
    }
    ]
}"""
#the problem was an extra comma after the last array entry
parsedSchema = schema.parse(MySchema)
print parsedSchema