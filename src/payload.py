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
        self.pause = lambda x: time.sleep(random() * x)
        self.log = "app_ec2q_log"
        self.tbl = "app_ec2q_tbl"

    @classmethod
    def from_sys_args(cls, sysargs):
        """Convenient constructor returns an instance of Payload.

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

    async def aws_ec2_cmd(self):
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
                raise ValueError(f"This EC2 instance is {state}!")
            self.pause(5)
            response = self.__aws_ec2_describe()
            state = response["InstanceStatuses"]["InstanceState"]["Name"]
        if time.time() > timeout:
            # TODO: place something here that wipes the transaction and cleans up
            raise TimeoutError(f"It took too long for this instance to {self.cmd}!")
        else:
            return self

    def __insert_new_id(self, conn):
        with conn.cursor() as curs:
            query = sql.SQL("INSERT INTO {} VALUES (%s, %s, %s); SELECT * FROM {} WHERE id = %s").format(sql.Identifier(self.tbl))
            values = (self.ec2, 0, True, self.ec2)
            curs.execute(query, values)
            record = curs.fetchone()
        return record

    def __update_queue(self, queue, conn):
        if self.cmd == "start":
            if queue == 0:
                self.__aws_ec2_cmd()
            queue += 1
        elif self.cmd == "stop":
            if queue == 1:
                self.__aws_ec2_cmd()
            queue -= 1
        with conn.cursor() as curs:
            query = sql.SQL("UPDATE {} SET queue = %s WHERE id = %s").format(sql.Identifier(self.tbl))
            curs.execute(query, (queue, self.ec2))
        return queue

    def process_request(self, conn):
        with conn.cursor() as curs:
            query = sql.SQL("SELECT * FROM {} WHERE id = %s").format(sql.Identifier(self.tbl))
            curs.execute(query, (self.ec2,))
            record = curs.fetchone()
        if not record:
            record = self.__insert_new_id(conn)
        queue = record[1]  # (id, queue, lock, time)
        queue = self.__update_queue(queue, conn)
        return self


# then we check the queue
# if queue == 0
# proceed and q++
# update the db
# then, trigger aws cli
