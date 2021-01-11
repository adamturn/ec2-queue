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
        self.log = "app_ec2q_log"
        self.tbl = "app_ec2q_tbl"

    @classmethod
    def from_sys_args(cls, sysargs):
        """Convenience constructor returning an instance of Payload.

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

    def __aws_ec2_cmd(self):
        if self.cmd == "start":
            requested_state, anti_state = "running", "stopped"
        elif self.cmd == "stop":
            requested_state, anti_state = "stopped", "running"
        response = self.__aws_ec2_describe()
        state = response["InstanceStatuses"]["InstanceState"]["Name"]
        timeout = time.time() + (60 * 2)  # 2 minutes from now
        while state != requested_state or time.time() <= timeout:
            if state == anti_state:
                try:
                    subprocess.run(
                        args=["aws", "ec2", f"{self.cmd}-instances", "--instance-ids", self.ec2, "--profile", self.prf],
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
        if time.time() > timeout:
            raise TimeoutError(f"Exceeded time limit for this instance to {self.cmd}!")
        else:
            return self

    def __queue_then_lock(self, conn):
        with conn.cursor() as curs:
            query = sql.SQL("SELECT * FROM {} WHERE id = %s;").format(sql.Identifier(self.tbl))
            curs.execute(query, (self.ec2,))
            # if we have this id
            if curs.rowcount == 1:
                record = curs.fetchone()
                app_lock = record[2]  # cols: id, queue, app_lock, last_update
                timeout = time.time() + (60 * 5)  # 5 minutes from now
                # queue until app is unlocked
                while app_lock or time.time() > timeout:
                    self.pause(5)
                    curs.execute()
                    record = curs.fetchone()
                    app_lock = record[2]
                if time.time() > timeout:
                    raise TimeoutError("Waited over 5 min for the app to unlock!")
                elif self.cmd == "start":
                    with conn.cursor() as curs:
                        query = sql.SQL(
                            "UPDATE {} SET queue = %s, app_lock = TRUE WHERE id = %s;".format(sql.Identifier(self.tbl))
                        )
                        queue = record[1] + 1
                        curs.execute(query, (queue, self.ec2))
                elif self.cmd == "stop":
                    with conn.cursor() as curs:
                        query = sql.SQL(
                            "UPDATE {} SET queue = %s, app_lock = TRUE WHERE id = %s;".format(sql.Identifier(self.tbl))
                        )
                        queue = record[1] - 1
                        curs.execute(query, (queue, self.ec2))
            # no id start: insert new record with queue and lock
            elif self.cmd == "start":
                with conn.cursor() as curs:
                    query = sql.SQL("INSERT INTO {} VALUES (%s, %s, %s);").format(sql.Identifier(self.tbl))
                    curs.execute(query, (self.ec2, 1, True))
            # no id stop: insert new record with no queue and lock
            elif self.cmd == "stop":
                with conn.cursor() as curs:
                    query = sql.SQL("INSERT INTO {} VALUES (%s, %s, %s);").format(sql.Identifier(self.tbl))
                    curs.execute(query, (self.ec2, 0, True))
        return self

    def __handle_new_ec2(self, conn):
        with conn.cursor() as curs:
            query = sql.SQL("INSERT INTO {} VALUES (%s, %s, %s); SELECT * FROM {} WHERE id = %s").format(sql.Identifier(self.tbl))
            values = (self.ec2, 0, True)
            curs.execute(query, values)
        return self

    def handle_start_request(self, conn):
        # then, check if queue == 0
        # if so: send the start request
        # regardless, queue ++
        with conn.cursor() as curs:
            query = sql.SQL("SELECT * FROM {} WHERE id = ")
        # self.__queue_then_lock(conn)
        self.__aws_ec2_cmd()
        # unlock
        with conn.cursor() as curs:
            query = sql.SQL("UPDATE {} SET app_lock = %s WHERE id = %s;").format(sql.Identifier(self.tbl))
            curs.execute(query, (False, self.ec2))
        return self
    
    def process_stop_task(self, conn):
        # we should have this id already
        with conn.cursor() as curs:
            curs.execute(sql.SQL("SELECT * FROM {} WHERE id = %s;").format(sql.Identifier(self.tbl)), (self.ec2,))
            if curs.rowcount == 1:
                # if we have it, then either the app is locked or it is not
                # if it is locked, we need to wait for it to unlock
                result = self.__queue_then_lock(curs)
                # once it is unlocked

        return self

    def process_task(self, conn):
        """Processes an Airflow task sending a command to the AWS CLI.

        Args:
            conn: active Psycopg2 connection object
        """
        with conn.cursor() as curs:
            query = sql.SQL("SELECT * FROM {} WHERE id = %s;").format(sql.Identifier(self.tbl))
            curs.execute(query, (self.ec2,))
            record = curs.fetchone()

        if not record:
            self.__handle_new_ec2(conn)

        if self.cmd == "start":
            self.handle_start_request(conn)
        elif self.cmd == "stop":
            self.process_stop_task(conn)
        else:
            raise ValueError(f"\'{self.cmd}\' is not a supported argument for cmd!")
        return self
