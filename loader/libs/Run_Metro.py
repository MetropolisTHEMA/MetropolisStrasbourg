import os, sys
import subprocess

def run_metro(runconfig):
    # creating a new run folder of number n+1 to store the newest run
    METRO_OUTPUT_DIR = runconfig["Run_outdir"]
    datapath= runconfig["Run_indir"]
    scriptpath = runconfig["Script_location"]

    if not os.path.exists(METRO_OUTPUT_DIR):
        os.mkdir(METRO_OUTPUT_DIR)
    outpath = "run_"+str(max((int(f[4:]) for f in os.listdir(METRO_OUTPUT_DIR) if f.startswith("run_")), default=0)+1)
    outpath = os.path.join(METRO_OUTPUT_DIR, outpath)
    os.mkdir(outpath)

    # set input data path

    # script path (leave as "" if your command is in the current folder)
    
    #add ".exe" if on windows
    syst = ""
    if (sys.platform == "win32" and not scriptpath.endswith(".exe")):
        syst=".exe"

    command = f"{scriptpath}{syst} --agents {datapath}\\agents.json  --parameters {datapath}\\parameters.json --road-network {datapath}\\network.json  --output {outpath}"
    subprocess.run(command, shell=True)
    print(f"Saved in {outpath}")
