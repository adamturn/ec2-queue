# EC2 Queue
Wraps around the AWS CLI. Instead of having Airflow tasks submit EC2 start/stop commands directly to the AWS CLI, we could send start/stop requests to this EC2 Queue app instead. 

Pass the EC2 instance id, AWS CLI command, and AWS CLI profile to `ec2`, `cmd` and `prf` arguments, respectively.

## Bare Metal Test
```shell
$ python src/main.py ec2=test cmd=start prf=default
$ python src/main.py ec2=test cmd=stop prf=default
```

## Airflow example
```python
t1 = SSHOperator(
    command = f"docker run --rm --name ec2q "
)
```

## CLI Documentation
Supported arguments.
### cmd
```
start: request to start ec2 instance
stop: request to stop ec2 instance
```