import pandas as pd
import sys
import os

sys.path.insert(0, "C:\\Users\\admin\\Documents\\GitHub\\SAGELauncher")

from SAVE_Outputs import upload_output_to_hana

if __name__ == '__main__':
    upload_output_to_hana(True)