"""
Python 3.6
Author: Adam Turner <turner.adch@gmail.com>
"""

# standard library
import pathlib
import re
import sys
# local modules
from process_task import Payload
from conndb import connect_postgres


def main():
    src_dir = pathlib.Path(__file__).parent.absolute()
    props_path = src_dir.parent / "cfg/config.properties"
    dbconn = connect_postgres(props_path)
    p = Payload.from_sys_args(sys.argv)
    p.process_task(dbconn)
    dbconn.close()  

    return None


if __name__ == "__main__":
    main()
