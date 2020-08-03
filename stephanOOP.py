#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module to create a .c360cfg file to be used for
setting the required configuration
"""
import os
import sys
from collections import OrderedDict

import shared.config as config
import shared.log
from util.string import append as strappend

class ConfigSetter(object):
    """ class to set the configuration """

    prerequisite_environment_variables = [
        "CB_HOST",
        "CB_USER",
        "CB_PASSWORD",
        "CB_BUCKET",
        "C360_ENVIRONMENT",
        "PROCESS_DATE",
        "ENCRYPTION_KEY"]

    environment = OrderedDict()
    c360cfg_filename = "~/.c360cfg"
    logger = None


    def check_prerequisites(self):
        """ check environment if mandatory variables are set """

        missing_variables = 0

        for variable in self.prerequisite_environment_variables:
            if not os.environ.get(variable, None):
                missing_variables += 1
                self.logger.log("Missing environment variable {}".format(variable))
            else:
                self.environment[variable] = os.environ.get(variable)

        return missing_variables


    def __init__(self):
        """ constructor """

        self.configuration = config.Configuration()
        self.logger = shared.log.Logger()
        self.__home_directory = os.path.expanduser("~")

        self.environment["PYTHONIOENCODING"] = "utf8"
        module_path = os.path.dirname(os.path.abspath(__file__))
        if module_path not in os.environ.get("PATH", ""):
            self.environment["PATH"] = "${PATH}:" + module_path

        if self.check_prerequisites() > 0:
            sys.exit(1)

        self.config = self.configuration.read_config()


    def write_ssh_key(self):
        """
        write ssh keys to the .ssh directory and set the correct
        permissions.
        """
        ssh_keys = (
            self
            .config
            .get("general", {})
            .get("ssh", {}))

        for key in ssh_keys:
            key_value = ssh_keys.get(key, {}).get("key")
            file_name = (
                "{}/.ssh/{}".format(
                    self.__home_directory,
                    ssh_keys.get(key, {}).get("filename")))

            with open(file_name, "w") as ssh:
                ssh.write("{}\n".format(key_value))

            # set correct permissions for ssh keys (600)
            os.chmod(file_name, 0o600)

        return


    def write_config(self):
        """
        write config to ~/.c360cfg file to be sourced so that the
        environment is set correctly
        """
        config_file_name = "{}/.c360cfg".format(self.__home_directory)

        with open(config_file_name, 'w') as config_file:
            for variable in self.environment:
                value = self.environment.get(variable, "")

                if variable in ["CB_PASSWORD", "ENCRYPTION_KEY"]:
                    quotes = "'"
                else:
                    quotes = '"'

                config_file.write(
                    "export {}={quote}{}{quote}\n".format(
                        variable, value, quote=quotes))
        self.logger.log("environment written to ~/.c360cfg")


    def write_s3_config(self):
        """ write .s3cfg for s3cmd """
        s3_config_file_name = "{}/.s3cfg".format(self.__home_directory)
        endpoint = self.environment.get("S3_ENDPOINT", "")
        if endpoint.startswith("https://"):
            endpoint = endpoint[8:]
        elif endpoint.startswith("http://"):
            endpoint = endpoint[7:]

        config_lines = [
            "[default]",
            "host_base = {}".format(endpoint),
            "host_bucket = {}".format(endpoint),
            "access_key = {}".format(
                self.environment.get("S3_ACCESSKEY", "")),
            "secret_key = {}".format(
                self.environment.get("S3_SECRETKEY", "ignored")),
            "use_https = true",
            "signature_v2 = false",
            "enable_multipart = false"]

        with open(s3_config_file_name, "w") as config_file:
            config_file.write(
                "\n".join(config_lines))


    def write_transfer_script(self):
        """
        write a file that copies data from s3 source storage to the
        temporary folder on the local filesystem.

        Prerequisites for the file to work:
            - target temporary directory needs to be available
            - target temporary directory needs to be accessible
              from all Spark nodes that do the processing
        """

        def write_to_file(file_name, lines, permissions=0700):
            """ write the lines of -lines- to file -file_name- """

            with open(file_name, "w") as out_file:
                out_file.write("\n".join(lines))

            os.chmod(file_name, permissions)

        entities = (
            self
            .config
            .get("processing", {})
            .get("entities", {}))

        temporary_path = (
            self.environment
            .get("C360_TEMP_PATH", "/nfsmount/")
            .replace("s3a:", "s3:")
            .replace("s3n:", "s3:"))
        source_path = (
            self.environment
            .get("C360_SOURCE_PATH", "/nfsmount/")
            .replace("s3a:", "s3:")
            .replace("s3n:", "s3:"))
        intermediate_path = (
            self.environment
            .get("C360_INTERMEDIATE_PATH", "/nfsmount/")
            .replace("s3a:", "s3:")
            .replace("s3n:", "s3:"))
        target_path = (
            self.environment
            .get("C360_TARGET_PATH", "/nfsmount/")
            .replace("s3a:", "s3:")
            .replace("s3n:", "s3:"))

        download_lines = [
            "#!/bin/bash",
            "mkdir -p {0}raw {0}parquet {0}json".format(temporary_path)]
        upload_lines = [
            "#!/bin/bash"]

        # handle index file
        if intermediate_path.startswith("s3"):
            identifier_filename = "identifier.parquet"
            download_lines.extend([
                '[ -d "{0}parquet/" ] && cd {0}parquet/ && pwd'.format(
                    temporary_path),
                "s3cmd get --force {0}{1}.tgz {1}.tgz".format(
                    intermediate_path,
                    identifier_filename),
                '[ -s "{0}.tgz" ] && tar -xzf {0}.tgz'.format(
                    identifier_filename),
                '[ -s "{0}.tgz" ] && rm {0}.tgz'.format(
                    identifier_filename)])

            upload_lines.extend([
                '[ -d "{0}parquet" ] && cd {0}parquet'.format(temporary_path),
                '[ -d "{0}" ] && tar -czf {0}.tgz {0}'.format(
                    identifier_filename),
                '[ -s "{1}.tgz" ] && s3cmd put {1}.tgz {0}{1}.tgz'.format(
                    intermediate_path,
                    identifier_filename)])

        # handle source files
        if source_path.startswith("s3"):
            download_lines.append(
                '[ -d "{0}raw" ] && cd {0}raw && pwd'.format(temporary_path))

            for entity in entities:
                if entity != "kpis":
                    download_lines.append(
                        "\n".join(
                            ["s3cmd get --force {0}{1} {1}".format(
                                source_path,
                                file_name)
                             for file_name in (
                                 entities
                                 .get(entity, {})
                                 .get("sourceFiles", []))]))
                else:
                    for kpi in entities.get(entity, {}):
                        download_lines.append(
                            "\n".join(
                                ["s3cmd get --force {0}{1} {1}".format(
                                    source_path,
                                    file_name)
                                 for file_name in (
                                     kpi
                                     .get("sourceFiles", []))]))

        # handle target files
        if target_path.startswith("s3"):
            upload_lines.append((
                'cd {0}json || (echo "Cannot find {0} for uploading final '
                'documents."; exit 1)').format(temporary_path))
            final_documents = [
                "person.json",
                "organization.json",
                "index.json",
                "event.json"]

            for document in final_documents:
                upload_lines.extend([
                    '[ -d "{0}" ] && tar -czf {0}.tgz {0}'.format(
                        document),
                    '[ -s "{1}.tgz" ] && s3cmd put {1}.tgz {0}{1}.tgz'.format(
                        target_path,
                        document)])

        download_file_name = "{}/download_files.sh".format(
            self.__home_directory)
        write_to_file(download_file_name, download_lines)

        upload_file_name = "{}/upload_files.sh".format(
            self.__home_directory)
        write_to_file(upload_file_name, upload_lines)


    def set_config(self):
        """ main method setting the configuration """
        self.assemble_environment()
        self.write_config()
        self.write_s3_config()
        self.write_transfer_script()
        self.write_ssh_key()


    def assemble_environment(self):
        """
        adds additional environment variables to environment
        in this order of precedence:

        - already set environment variables
        - values from the configuration in the database
        - default values
        """

        spark_host = ""
        spark_port = ""
        with open("{}/.bashrc".format(self.__home_directory)) as bashrc:
            for line in bashrc:
                if "SPARK_MASTER_HOST=" in line:
                    spark_host = line.split("=", 1)[1].strip("\n")
                if "SPARK_MASTER_PORT=" in line:
                    spark_port = line.split("=", 1)[1].strip("\n")
                if "SPARK_MASTER=" in line:
                    spark_master = line.split("=", 1)[1].strip("\n")

            if spark_master and not "$" in spark_master:
                spark_master = (
                    spark_master
                    .replace("$SPARK_MASTER_HOST", spark_host)
                    .replace("$SPARK_MASTER_PORT", spark_port))
            else:
                if spark_host and spark_port:
                    spark_master = (
                        "spark://{}:{}".format(spark_host, spark_port))
                else:
                    self.logger.log("spark host: {}".format(spark_host))
                    self.logger.log("spark port: {}".format(spark_port))
                    self.logger.log("spark master: {}".format(spark_master))
                    self.logger.error("Cannot build spark master.")

        self.environment["SPARK_MASTER"] = spark_master

        self.environment["C360_CLIENT"] = (
            os.environ.get("C360_CLIENT",
                           self
                           .config
                           .get("general", {})
                           .get("client",
                                os.environ.get("C360_ENVIRONMENT", ""))))

        self.environment["CB_DATA_BUCKET"] = (
            os.environ.get(
                "CB_DATA_BUCKET",
                self
                .config
                .get("general", {})
                .get("couchbase", {})
                .get("bucket", "c360")))

        self.environment["CB_INDEX_BUCKET"] = (
            os.environ.get(
                "CB_INDEX_BUCKET",
                self
                .config
                .get("general", {})
                .get("couchbase", {})
                .get("index", "c360")))

        self.environment["CB_STATISTICS_BUCKET"] = (
            os.environ.get(
                "CB_STATISTICS_BUCKET",
                self
                .config
                .get("general", {})
                .get("couchbase", {})
                .get("statistics", "c360_statistics"))
        )

        self.environment["S3_ENDPOINT"] = (
            os.environ.get(
                "S3_ENDPOINT",
                self
                .config
                .get("general", {})
                .get("s3", {})
                .get("endpoint", "")))

        self.environment["S3_ACCESSKEY"] = (
            os.environ.get(
                "S3_ACCESSKEY",
                self
                .config
                .get("general", {})
                .get("s3", {})
                .get("accesskey", "")))

        self.environment["S3_SECRETKEY"] = (
            os.environ.get(
                "S3_SECRETKEY",
                self
                .config
                .get("general", {})
                .get("s3", {})
                .get("secretkey", "ignored")))

        self.environment["C360_SOURCE_PATH"] = strappend(
            self
            .config
            .get("processing", {})
            .get("paths", {})
            .get("source", {}), "/")

        self.environment["C360_INTERMEDIATE_PATH"] = strappend(
            self
            .config
            .get("processing", {})
            .get("paths", {})
            .get("intermediate", {}), "/")

        self.environment["C360_TARGET_PATH"] = strappend(
            self
            .config
            .get("processing", {})
            .get("paths", {})
            .get("final", ""), "/")

        self.environment["C360_TEMP_PATH"] = strappend(
            self
            .config
            .get("processing", {})
            .get("internal", {})
            .get("temporary", ""), "/")

        self.environment["C360_LOCAL_SOURCE_PATH"] = strappend(
            self
            .configuration
            .get_local_path(
                self
                .config
                .get("processing", {}), "source"), "/")

        self.environment["C360_LOCAL_INTERMEDIATE_PATH"] = strappend(
            self
            .configuration
            .get_local_path(
                self
                .config
                .get("processing", {}), "intermediate"), "/")

        self.environment["C360_LOCAL_TARGET_PATH"] = strappend(
            self
            .configuration
            .get_local_path(
                self
                .config
                .get("processing", {}), "final"), "/")


def main():
    """ main class reading the config and writing it into a file """
    config_setter = ConfigSetter()
    config_setter.set_config()

if __name__ == '__main__':
    main()