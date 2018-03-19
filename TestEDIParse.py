import av.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter

schemafile = "C:\Users\stephen.meyer\PycharmProjects\AnthemSearchPoC\edi.avsc"
schema = avro.schema.parse(open(schemafile),"rb").read()

inputfile = "C:\Users\stephen.meyer\Documents\Client Work\Anthem\Data Consumption\SearchPoC\editest.avro"
reader = DataFileReader (open(inputfile ,"rb"),DatumReader())
for EDIClaims in reader:
    print EDIClaims
reader.close()

