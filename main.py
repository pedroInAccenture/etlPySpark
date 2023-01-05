import configparser
import os
import etls.local2SF as l2sf
import sys

def getConfig(configFileName):
    print("===> reading config file")
    config = configparser.ConfigParser()
    ini_path = os.path.join(os.getcwd(), configFileName)
    config.read(ini_path)
    return config['Snowflake']


def getCredentialsSF(configFileName):
    config = getConfig(configFileName)
    credentials = {
        "sfUrl": config['SF_ACCOUNT'] + ".snowflakecomputing.com",
        "sfuser": config['SF_USER'],
        "sfPassword": config['SF_PASS'],
        "sfRole": config['SF_ROLE'],
        "sfDatabase": config['SF_DATABASE'],
        "sfSchema": config['SF_SCHEMA'],
        "sfWarehouse": config['SF_WAREHOUSE']
    }
    return credentials


if __name__ == "__main__":
    snowflake_credentials = getCredentialsSF(sys.argv[1])

    l2sf.execute(snowflake_credentials)
