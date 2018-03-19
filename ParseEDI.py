import struct, io, pprint
import avro
import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
import json
import collections
import numpy
import pandas

schemafile = "C:\Users\stephen.meyer\PycharmProjects\AnthemSearchPoC\AvroSchema\edi.avsc"
schema = avro.schema.parse(open(schemafile,"rb").read())

outputfile = "C:\Users\stephen.meyer\Documents\Client Work\Anthem\Data Consumption\SearchPoC\editest.avro"
writer = DataFileWriter (open(outputfile,"wb"),DatumWriter(),schema)

inputfile = "C:\Users\stephen.meyer\Documents\Client Work\Anthem\Data Consumption\SearchPoC\NASCO_EDI_CLM_PROF_20180212063513.dat"

#these are the conversion definitions for the file specs if need to convert to something other than string, e.g. integer or float datatype
cnv_text = str.rstrip
cnv_int = int

#Let's do this thing
recordid = ""

class Record(object):
    pass #just need something to support the row object below

outputrecord = '{"EDIClaims": ['
firstrecord = True

with open(inputfile,"rb") as filein:
    for linestring in filein:
        linestring = linestring.replace("{","0")
        #print linestring
        recordid = linestring[0:14]
        #print recordid
        recordnum = linestring[14:17]
        #print recordnum
        recordmod = linestring[20:21]
        #print recordmod
        recordtype = recordnum + recordmod
        #print recordtype

        if recordtype == "000A": # file header
            lastrec = False

        elif recordtype == "400A":  # 400A stuff
            outputrecord += '{"rowid": "' + recordid + '", "inputfile": "' + inputfile +'"}, '
            outputrecord += '{ "name": "claimheader400a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("header-sender-id", 24, 15),
                          ("prod-or-test", 39, 1),
                          ("group-submitter-id", 40, 15),
                          ("group-receiver-id", 55, 15),
                          ("hipaa-release", 70, 35),
                          ("received-date", 105, 8),
                          ("claim-or-encounter-ind", 113, 2),
                          ("service-category", 115, 1),
                          ("brand-ind", 116, 1),
                          ("claim-routed-to", 117, 2),
                          ("compliance-expected", 119, 1),
                          ("submitter-type", 120, 3),
                          ("submitter-id-qual", 123, 2),
                          ("submitter-id", 125, 40),
                          ("submitter-name-type", 165, 1),
                          ("submitter-name-last", 166, 60),
                          ("submitter-name-first", 226, 35),
                          ("submitter-name-middle", 261, 25),
                          ("receiver-receiver-id", 286, 40),
                          ("medicare-crossover-ind", 326, 1),
                          ("enterprise-edi-dcn", 327, 19),
                          ("local-system-dcn-1", 346, 20),
                          ("local-system-dcn-2", 366, 20),
                          ("sender-group-control", 386, 9),
                          ("filler", 395, 30)
                          ]
            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "400B": # 400B stuff
            outputrecord += '{ "name": "submittercontactinfo400b", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("submitr-contact-name", 24, 60),
                          ("submitr-contact-qual", 84, 2),
                          ("submitr-contact-number", 86, 256),
                          ("filler", 342, 38)]
            fieldspecs.sort(key=lambda x: x[1])


        elif recordtype == "401A": # 401A stuff
            outputrecord += '{ "name": "billingprovider401a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("provider-type", 24, 3),
                          ("name-type", 27, 1),
                          ("name-last-or-org", 28, 60),
                          ("name-first", 88, 35),
                          ("name-middle", 123, 25),
                          ("name-suffix", 148, 10),
                          ("provider-id-qual", 158, 2),
                          ("provider-id", 160, 40),
                          ("street-address-1", 200, 55),
                          ("street-address-2", 255, 55),
                          ("city-name", 310, 30),
                          ("state-code", 340, 2),
                          ("zip-code", 342, 15),
                          ("country-code", 357, 3),
                          ("country-subdivision", 360, 3),
                          ("provider-other-id-tbl", 363, 159),
                          ("taxonomy-code-list-id", 522, 3),
                          ("taxonomy-code", 525, 25),
                          ("currency-code", 550, 3),
                          ("filler", 553, 57)]

            fieldspecs.sort(key=lambda x: x[1])


        elif recordtype == "421A": # 421A stuff
            outputrecord += '{ "name": "subscriber421a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("subscriber-type", 24, 3),
                          ("payer-responsblty-seq", 27, 1),
                          ("relationship-code", 28, 2),
                          ("group-number", 30, 50),
                          ("group-name", 80, 60),
                          ("insurance-type", 140, 3),
                          ("claim-filing-indicator", 143, 2),
                          ("name-type", 145, 1),
                          ("name-last-or-org", 146, 60),
                          ("name-first", 206, 35),
                          ("name-middle", 241, 25),
                          ("name-suffix", 266, 10),
                          ("membership-id-qual", 276, 2),
                          ("membership-id", 278, 40),
                          ("street-address-1", 318, 55),
                          ("street-address-2", 373, 55),
                          ("city-name", 428, 30),
                          ("state-code", 458, 2),
                          ("zip-code", 460, 15),
                          ("country-code", 475, 3),
                          ("country-subdivision", 478, 3),
                          ("date-of-birth", 481, 8),
                          ("gender-code", 489, 1),
                          ("subscrbr-other-id-tbl", 490, 106),
                          ("its-relationship-code", 596, 2),
                          ("its-sec-payor-prc-ind", 598, 1),
                          ("filler", 599, 61)]
            
            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "422A": # 422A stuff
            outputrecord += '{ "name": "payer422a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("payer-type", 24, 3),
                          ("name-type", 27, 1),
                          ("name-last-or-org", 28, 60),
                          ("payer-id-qual", 88, 2),
                          ("payer-id  ", 90, 40),
                          ("street-address-1", 130, 55),
                          ("street-address-2", 185, 55),
                          ("city-name", 240, 30),
                          ("state-code", 270, 2),
                          ("zip-code", 272, 15),
                          ("country-code", 287, 3),
                          ("country-subdivision", 290, 3),
                          ("payer-other-id-table", 293, 265),
                          ("filler", 558, 57)]

            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "441A": # 441A stuff
            outputrecord += '{ "name": "patient441a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("date-of-death", 24, 8),
                          ("patient-weight-qual", 32, 2),
                          ("patient-weight", 34, 10),
                          ("pregnancy-indicator", 44, 1),
                          ("relationship-code", 45, 2),
                          ("patient-type", 47, 3),
                          ("name-type", 50, 1),
                          ("name-last-or-org", 51, 60),
                          ("name-first", 111, 35),
                          ("name-middle", 146, 25),
                          ("name-suffix", 171, 10),
                          ("street-address-1", 181, 55),
                          ("street-address-2", 236, 55),
                          ("city-name", 291, 30),
                          ("state-code", 321, 2),
                          ("zip-code", 323, 15),
                          ("country-code", 338, 3),
                          ("country-subdivision", 341, 3),
                          ("date-of-birth", 344, 8),
                          ("gender-code", 352, 1),
                          ("its-relationship-code", 353, 2),
                          ("its-employment-status", 355, 1),
                          ("filler", 356, 39)]
            
            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "461A": # 461A stuff
            outputrecord += '{ "name": "claim-basic461a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("patient-account-number", 24, 38),
                          ("total-charges", 62, 10),
                          ("type-of-bill", 72, 3),
                          ("facility-code-qual", 75, 1),
                          ("service-category", 76, 1),
                          ("provider-signature-ind", 77, 1),
                          ("provider-accpts-assign", 78, 1),
                          ("benft-assignd-auth-ind", 79, 1),
                          ("release-of-info-ind", 80, 1),
                          ("patient-signature-src", 81, 1),
                          ("related-causes-table", 82, 6),
                          ("related-causes-state", 88, 2),
                          ("related-causes-country", 90, 3),
                          ("special-program-code", 93, 3),
                          ("provider-agreemnt-code", 96, 1),
                          ("predeterm-benefit-code", 97, 2),
                          ("delay-reason-code", 99, 2),
                          ("admission-date-qual", 101, 3),
                          ("admission-date", 104, 8),
                          ("admission-time", 112, 4),
                          ("discharge-date-qual", 116, 3),
                          ("discharge-date", 119, 8),
                          ("discharge-time", 127, 4),
                          ("statement-date-qual", 131, 3),
                          ("statement-from-date", 134, 8),
                          ("statement-thru-date", 142, 8),
                          ("onset-date-qual", 150, 3),
                          ("onset-date", 153, 8),
                          ("ortho-trtmt-mos", 161, 7),
                          ("ortho-trtmt-mos-rem", 168, 7),
                          ("orthodonture-ind", 175, 1),
                          ("tooth-status-table", 176, 350),
                          ("admission-type", 526, 1),
                          ("admission-source", 527, 1),
                          ("patient-status", 528, 2),
                          ("contract-type", 530, 2),
                          ("contract-amt", 532, 10),
                          ("contract-percent", 542, 6),
                          ("contract-code", 548, 25),
                          ("contract-discnt-pct", 573, 6),
                          ("contract-version-id", 579, 15),
                          ("specified-amt-qual", 594, 3),
                          ("specified-amt", 597, 10),
                          ("filler", 607, 63)]
            
            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "461F": # 461F stuff
            outputrecord += '{ "name": "claim-levelspecialreferences461f", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("reference-number-table", 24, 424),
                          ("filler", 448, 47)]
            
            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "461L": # 461L stuff
            outputrecord += '{ "name": "diagnosiscodes461l", "fields": '
            lastrec = False
        
            fieldspecs = [("record-aggregator", 0, 14),
                    ("record-type", 14, 3),
                    ("record-type-seq", 17, 3),
                    ("record-type-qual", 20, 1),
                    ("record-type-qual-seq", 21, 3),
                    ("diagnosis-table", 24, 270),
                    ("drg-code-qual", 294, 3),
                    ("drg-code", 297, 6),
                    ("filler", 303, 32)]

            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "465A": # 465A stuff
            outputrecord += '{ "name": "serviceline-basic465a", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("procedure-code-qual", 24, 2),
                          ("procedure-code", 26, 10),
                          ("procedure-modifr-table", 36, 8),
                          ("procedure-description", 44, 80),
                          ("line-charge", 124, 10),
                          ("service-units-qual", 134, 2),
                          ("service-units", 136, 11),
                          ("facility-code", 147, 2),
                          ("diag-code-pointer-tbl", 149, 8),
                          ("emergency-ind", 157, 1),
                          ("early-psdt-ind", 158, 1),
                          ("family-planning-ind", 159, 1),
                          ("copay-status", 160, 1),
                          ("dme-procedur-code-qual", 161, 2),
                          ("dme-procedur-code", 163, 10),
                          ("dme-days-qual", 173, 2),
                          ("dme-days ", 175, 7),
                          ("dme-rental-amount", 182, 10),
                          ("dme-purchase-amount", 192, 10),
                          ("dme-rental-frequency", 202, 1),
                          ("dme-certificate-type", 203, 1),
                          ("dme-duration-qual", 204, 2),
                          ("dme-duration", 206, 7),
                          ("service-date-qual", 213, 3),
                          ("service-from-date", 216, 8),
                          ("service-thru-date", 224, 8),
                          ("contract-type", 232, 2),
                          ("contract-amount", 234, 10),
                          ("contract-percent", 244, 6),
                          ("contract-code", 250, 25),
                          ("contract-discnt-pct", 275, 6),
                          ("contract-version-id", 281, 15),
                          ("sales-tax-qual", 296, 3),
                          ("sales-tax-amt", 299, 10),
                          ("postage-claimed-qual", 309, 3),
                          ("postage-claimed-amt", 312, 10),
                          ("purchasd-svc-provider", 322, 25),
                          ("purchasd-svc-amt", 347, 10),
                          ("filler", 357, 38)]

            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "465L": # 465L stuff
            outputrecord += '{ "name": "service-levelspecialreferences465l", "fields": '
            lastrec = False

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("reference-number-table", 24, 212),
                          ("filler", 236, 24)]

            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "499X": # 499X stuff
            outputrecord += '{ "name": "claimtrailer499x", "fields": '
            lastrec = True

            fieldspecs = [("record-aggregator", 0, 14),
                          ("record-type", 14, 3),
                          ("record-type-seq", 17, 3),
                          ("record-type-qual", 20, 1),
                          ("record-type-qual-seq", 21, 3),
                          ("claim-record-count", 24, 7),
                          ("claim-service-count", 31, 3),
                          ("sum-of-line-charges", 34, 11),
                          ("filler", 45, 5)]

            fieldspecs.sort(key=lambda x: x[1])

        elif recordtype == "900A": # 900A stuff
            lastrec = True
        else:   # Not a defined type... ignore for now?
            pass

        print outputrecord
        if recordtype not in ["000A","900A"]:

            unpack_len = 0
            unpack_fmt = ""

            #returnspec = fieldspecs(recordtype)
            #print recordtype, (returnspec)

            for fieldspec in fieldspecs:
                start = fieldspec[1]
                end = start + fieldspec[2]
                if start > unpack_len:
                    unpack_fmt += str(start - unpack_len) + "x"
                unpack_fmt += str(end - start) + "s"
                unpack_len = end
            field_indices = range(len(fieldspecs))
            print recordid, recordtype, unpack_len, unpack_fmt, field_indices

            unpacker = struct.Struct(unpack_fmt).unpack_from

            raw_fields = unpacker(linestring)
            r = Record()
            for x in field_indices:
              setattr(r, fieldspecs[x][0], str.rstrip(raw_fields[x]))

            pd = pandas.DataFrame([r])
            outputrecord += pd.to_json(orient='records')
            #print outputrecord

        if recordtype != "000A":
            if not lastrec:
                outputrecord += ' }'
                outputrecord += ','

        #this should be it!
        #Now write the final output to the target file

#if we get here, the file is processed and done...
outputrecord += '} ] }'
#print outputrecord

outputrecord = outputrecord.replace("""{"0":""",'')
outputrecord = outputrecord.replace('}}]','}]')

print outputrecord
writer.append(outputrecord)
print "File name: " + inputfile + " is complete."
writer.close()