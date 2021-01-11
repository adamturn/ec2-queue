"""
Python 3.6
Author: Adam Turner <turner.adch@gmail.com>
"""

# python package index
from psycopg2 import sql
# standard library
import io
import json
import subprocess
import time
from datetime import datetime
from random import random

class Payload(object):

    def __init__(self, ec2, cmd, prf):
        self.ec2 = ec2
        self.cmd = cmd
        self.prf = prf
        self.pause = lambda x: time.sleep(random() * x)  # 0 to x seconds
        self.tblname = "app_ec2_queue"
        self.tblcols = {"id": "text", "app_lock": "bool", "queue": "integer", "last_update": "timestamp"}

    @classmethod
    def from_sys_args(cls, sysargs):
        """Convenience constructor.

        Args:
            sysargs: expects sys.argv list
        """
        delim = "="
        cfg = {argv.split(delim)[0].strip(): argv.split(delim)[1].strip() for argv in sysargs if delim in argv}
        return cls(ec2=cfg["ec2"], cmd=cfg["cmd"], prf=cfg["prf"])

    def __aws_cli_error(self):
        print("SUBPROCESS ERROR: AWS CLI returned an error! Check STDOUT above to see what went wrong.")
        return self

    def __aws_ec2_describe(self):
        with io.StringIO() as buffer:
            try:
                subprocess.run(
                    args=["aws", "ec2", "describe-instance-status", "--instance-id", self.ec2],
                    check=True,
                    stdout=buffer
                )
            except subprocess.CalledProcessError as err:
                self.__aws_cli_error()
                raise err
            else:
                response = json.load(buffer)
        return response

    def __aws_ec2_start(self):
        response = self.__aws_ec2_describe()
        state = response["InstanceStatuses"]["InstanceState"]["Name"]
        timeout = time.time() + 60  # 1 minute from now
        while state != "running" or time.time() > timeout:
            if state == "stopped":
                try:
                    subprocess.run(
                        args=["aws", "ec2", "start-instances", "--instance-ids", self.ec2, "--profile", self.prf],
                        check=True
                    )
                except subprocess.CalledProcessError as err:
                    self.__aws_cli_error()
                    raise err
            elif state in ("shutting-down", "terminated"):
                raise ValueError("This AWS EC2 instance is " + state)
            self.pause(5)
            response = self.__aws_ec2_describe()
            state = response["InstanceStatuses"]["InstanceState"]["Name"]
        return self

    def __aws_ec2_stop(self):
        response = self.__aws_ec2_describe()
        state = response["InstanceStatuses"]["InstanceState"]["Name"]
        timeout = time.time() + 60  # 1 minute from now
        while state != "stopped" or time.time() > timeout:
            if state == "running":
                try:
                    subprocess.run(
                        args=["aws", "ec2", "stop-instances", "--instance-ids", self.ec2, "--profile", self.prf],
                        check=True
                    )
                except subprocess.CalledProcessError as err:
                    self.__aws_cli_error()
                    raise err
            elif state in ("shutting-down", "terminated"):
                raise ValueError(f"This AWS EC2 instance is {state}!")
            self.pause(5)
            response = self.__aws_ec2_describe()
            state = response["InstanceStatuses"]["InstanceState"]["Name"]
        return self

    def __handle_app_lock(self, curs):
        result = curs.fetchone()
        app_lock = result[2]
        timeout = time.time() + (60 * 5)  # 5 minutes from now
        while app_lock or time.time() > timeout:
            self.pause(5)
            curs.execute()
            result = curs.fetchone()
            app_lock = result[2]
        if time.time() > timeout:
            raise TimeoutError("Waited over 5 min for the app to unlock!")
        else:
            return result

    def __update_queue(self, queue, conn):
        with conn.cursor() as curs:
            query = sql.SQL(
                "UPDATE {} SET queue = %s, app_lock = True WHERE id = %s;".format(sql.Identifier(self.tblname))
            )
            curs.execute(query, (queue, self.ec2))
        return self

    def process_start_task(self, conn):
        # check if we have this instance id
        with conn.cursor() as curs:
            curs.execute(sql.SQL("SELECT * FROM {} WHERE id = %s;").format(sql.Identifier(self.tblname)), (self.ec2,))
            if curs.rowcount == 1:
                result = self.__handle_app_lock(curs)
                queue = result[1] + 1
                self.__update_queue(queue, conn)
            else:
                with conn.cursor() as curs:
                    query = sql.SQL("INSERT INTO {} VALUES (%s, %s);").format(sql.Identifier(self.tblname))
                    curs.execute(query, (self.ec2, 1))
        self.__aws_ec2_start()
        with conn.cursor() as curs:
            query = sql.SQL("UPDATE {} SET app_lock = %s WHERE id = %s;").format(sql.Identifier(self.tblname))
            curs.execute(query, (False, self.ec2))
                
        return self
    
    def process_stop_task(self, conn):

        return self

    def process_task(self, conn):
        """Processes an Airflow task sending a start request to the AWS CLI.

        Args:
            conn: active Psycopg2 connection object
        """
        if self.cmd == "start":
            self.process_start_task(conn)
        elif self.cmd == "stop":
            self.process_stop_task(conn)
        else:
            raise ValueError(f"\'{self.cmd}\' is not a supported argument for cmd!")
        return self
