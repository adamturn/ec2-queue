import subprocess
import time

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
