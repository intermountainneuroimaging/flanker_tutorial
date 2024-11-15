from pathlib import Path
import subprocess as sp
import sys, os, nipype, datetime, logging
import flywheel, flywheel_gear_toolkit
from datetime import datetime
from datetime import date
import glob
import pandas as pd
from time import sleep
import time
import re
import tempfile
from zipfile import ZipFile

fw = flywheel.Client('')
log = logging.getLogger(__name__)


def run_gear(gear, config, inputs, tags, dest, analysis_label=[]):
    """Submits a job with specified gear and inputs.
    
    Args:
        gear (flywheel.Gear): A Flywheel Gear.
        config (dict): Configuration dictionary for the gear.
        inputs (dict): Input dictionary for the gear.
        tags (list): List of tags for gear
        dest (flywheel.container): A Flywheel Container where the output will be stored.
        analysis_label (str): label for gear.
        
    Returns:
        str: The id of the submitted job (for utility gear) or analysis container (for analysis gear).
        
    """
    try:
        # Run the gear on the inputs provided, stored output in dest constainer and returns job ID
        if not analysis_label:
            label = gear['gear']['name']+datetime.now().strftime(" %x %X")
        else:
            label = analysis_label
        gear_job_id = gear.run(analysis_label=label, config=config, inputs=inputs, tags=tags, destination=dest)
        log.debug('Submitted job %s', gear_job_id)
        
        return gear_job_id
    except flywheel.rest.ApiException:
        log.exception('An exception was raised when attempting to submit a job for %s',
                      gear_job_id.name)
        
        
def session_analysis_exists(container, gear_info, status=["complete","running","pending"], status_bool_type="any", count_up_to_failures=1):
    # Returns True if analysis already exists with a running or complete status, else false
    # make sure to pass full session object (use fw.get_session(session.id))
    #
    #Get all analyses for the session
    flag=False
    counter=0
    
    #handle checks for any version of gear or specific version 
    if "/" in gear_info:
        gear_name = gear_info.split("/")[0]
        gear_version = gear_info.split("/")[1]  # allow wildcard expressions here to check for multiple gear versions 
    else:
        gear_name = gear_info
        gear_version = []
    
    # check all session analyses
    for analysis in container.analyses:
        if not analysis.gear_info:
            continue
        #only print ones that match the analysis label
        if gear_name == analysis.gear_info.name:
            if gear_version:
                r1 = re.compile(gear_version)
                if not r1.search(analysis.gear_info["version"]):
                    continue
                    
            #filter for only successful job
            analysis_job=analysis.job
            if any(analysis_job.state in string for string in status):
                if analysis_job.state == "failed":
                    counter += 1
                    if counter >= count_up_to_failures:
                        flag=True
                else:
                    flag=True
            else:
                # if any of the analyses that match name and version, but do not match status
                if status_bool_type == "all":
                    return False
    
    return flag


def find_analysis(container, gear_info, label=None, status=["complete","running","pending"]):
    # Returns analysis object if exists by analysis name in that container
    # make sure to pass full session object (use fw.get_session(session.id))
    #
   
    #handle checks for any version of gear or specific version 
    if "/" in gear_info:
        gear_name = gear_info.split("/")[0]
        gear_version = gear_info.split("/")[1]  # allow wildcard expressions here to check for multiple gear versions 
    else:
        gear_name = gear_info
        gear_version = []
    
    #Get all analyses for the session
    analys_obj = None
    
    # check all session analyses
    for analysis in container.analyses:
        
        if not analysis.gear_info:
            continue
            
        #only print ones that match the analysis label
        if gear_name == analysis.gear_info.name:
            if gear_version:
                r1 = re.compile(gear_version)
                if not r1.search(analysis.gear_info["version"]):
                    continue
            if label and label not in analysis.label:
                continue
            analysis_job=analysis.job
            if not hasattr(analysis_job,'state'): 
                analys_obj = analysis
            else:
                if any(analysis_job.state in string for string in status):
                    analys_obj = analysis
    
    return analys_obj



def holdjob(jobids, timeout, period=0.25):
    mustend = time.time() + timeout
    while time.time() < mustend:
        if isinstance(jobids, str):
            jobid =  [jobids]
        
        for jid in jobids:
            anlys = fw.get_job(jid)
            
            if any(anlys.state.lower() == s for s in ['complete','cancelled','failed']): 
                log.info('Job %s: completed with status: %s', anlys.id, anlys.state) 
                jobids.remove(jid)
        
        if isempty(jobids):
            return True
    
        time.sleep(period)
        log.info('Job %s: timeout after %s seconds... continuing run script', jobid.id, timeout)
        
    return False


def hasacquisition(session,acq_name):
    for acq in session.acquisitions.find():
        if acq_name in acq.label:
            return True
    
    return False


def searchfiles(path, dryrun=False, find_first=False):
    cmd = "ls -d " + path

    log.debug("\n %s", cmd)

    if not dryrun:
        terminal = sp.Popen(
            cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True
        )
        stdout, stderr = terminal.communicate()
        log.debug("\n %s", stdout)
        log.debug("\n %s", stderr)

        files = stdout.strip("\n").split("\n")

        if find_first:
            files = files[0]

        return files
    
    
os.environ["FLYWHEEL_SDK_REQUEST_TIMEOUT"]="6000"

def upload_file_to_container(conatiner, fp, overwrite=False, update=True, replace_info=[], **kwargs):
    """Upload file to FW container and update info if `update=True`
    
    Args:
        container (flywheel.Project): A Flywheel Container (e.g. project, analysis, acquisition)
        fp (Path-like): Path to file to upload
        update (bool): If true, update container with key/value passed as kwargs.        
        kwargs (dict): Any key/value properties of Acquisition you would like to update.        
    """
    basename = os.path.basename(fp)
    if not os.path.isfile(fp):
        raise ValueError(f'{fp} is not file.')
        
    if conatiner.get_file(basename) and not overwrite:
        log.info(f'File {basename} already exists in container. Skipping.')
        return
    
    if conatiner.get_file(basename) and overwrite:
        log.info(f'File {basename} already exists, overwriting.')
        fw.delete_container_file(conatiner.id,basename)
        time.sleep(10)
        
    log.info(f'Uploading {fp} to container {conatiner.id}')
    conatiner.upload_file(fp)
    while not conatiner.get_file(basename):   # to make sure the file is available before performing an update
        conatiner = conatiner.reload()
        time.sleep(1)
    
    f = conatiner.get_file(basename)
    
    if replace_info:
        f.replace_info(replace_info)
        log.info(f'Replacing info {info.keys()} to acquisition {acquistion.id}')
        
    if update and kwargs:
        f.update(**kwargs)

        
def get_table(pycontext):
    
    log.info("Using Configuration Settings: ")
    log.parent.handlers[0].setFormatter(logging.Formatter('\t\t%(message)s'))
    
    log.info("project: %s", str(pycontext["project"]))
    log.info("gear name: %s", str(pycontext["gear"]))
    if "version" in pycontext:
        log.info("gear version: %s", str(pycontext["version"]))
    if "regex" in pycontext:
        log.info("analysis label regex: %s", str(pycontext["regex"]))
    log.parent.handlers[0].setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(message)s'))
    
    summary=pd.DataFrame()

    # find flywheel project
    project = fw.projects.find_one('label='+pycontext["project"])
    
    # get full flywheel object
    project = fw.get_project(project.id)
    
    # get gear-name
    gear = pycontext["gear"]
    
    # loop session in project
    for ses in project.sessions.find():

        full_session=fw.get_session(ses.id)

        if  "pilot" in full_session.tags:
            continue
        
        # loop analyses in session TODO: Find a more generic walker strategy
        for analysis in full_session.analyses:
            
            # only explore flywheel jobs (not uploads)
            if not analysis.job:
                continue

        for analysis in full_session.analyses:
            if not analysis.job:
                continue
            
            #only print ones that match the analysis label
            if pycontext["gear"] == analysis.gear_info.name:
                if "version" in pycontext:
                    r1 = re.compile(pycontext["version"])
                    if not r1.search(analysis.gear_info["version"]):
                        continue
                if "regex" in pycontext and pycontext["regex"] not in analysis.label:
                    continue
                
                # we met all conditions, store in table now
                df=pd.DataFrame({"timestamp":full_session.timestamp, 
                                 "subject.label":full_session.subject.label, 
                                 "session.label":full_session.label,
                                 "session.id": str(full_session.id),
                                 "project": fw.get_container(full_session.project).label,
                                 "Run Downstream Analyses": "COMPLETENESS" in full_session.info and full_session.info["COMPLETENESS"]["Run Downstream Analyses"] or None,
                                 "gear.name": analysis.gear_info.name,
                                 "gear.version": analysis.gear_info["version"],
                                 "analysis.label": analysis.label,
                                 "analysis.state": analysis.job.state,
                                 "analysis.id": analysis.id,
                                 "cli.cmd": 'fw download -o download.zip "{}/{}/{}/{}/{}"'.format(fw.get_container(full_session.project).label,full_session.subject.label, full_session.label,"analyses",analysis.label),
                                 "Notes": " ".join([x["text"] for x in full_session.notes])}, index=[0])
            


                summary = pd.concat([summary, df])

    summary = summary.sort_values('timestamp', ignore_index = True)
    
    return summary


def download_session_analyses_byid(analysis_id, download_path):
    # loop through all sessions in the project. More detailed filters could be 
    #   used to specify a subset of sessions
        
    analysis = fw.get_container(analysis_id)

    full_session = fw.get_container(analysis["parents"]["session"])

    if analysis:
        for fl in analysis.files:
            if '.zip' in fl['name']:
                download_and_unzip_inputs(analysis, fl, download_path)
            else:
                os.makedirs(os.path.join(download_path,'files'), exist_ok=True)
                fl.download(os.path.join(download_path,'files',fl['name']))

        log.info('Downloaded analysis: %s for Subject: %s Session: %s', analysis.label,full_session.subject.label, full_session.label)      
    else:
        log.info('Analysis not found: for Subject: %s Session: %s', full_session.subject.label, full_session.label)  

        
def download_and_unzip_inputs(parent_obj, file_obj, path):
    """
    unzip_inputs unzips the contents of zipped gear output into the working
    directory.
    Args:
        zip_filename (string): The file to be unzipped
    """
    rc = 0
    outpath = []
    
    os.makedirs(path, exist_ok=True)
    
    # start by checking if zipped file
    if ".zip" not in file_obj.name:
        return
    
    # next check if the zip file is organized with analysis id as top dir
    zip_info = parent_obj.get_file_zip_info(file_obj.name)
    zip_top_dir = os.path.dirname(zip_info.members[0].path)
    if len(zip_top_dir)==24:
        # this is an archive zip and needs to be handled special
        with tempfile.TemporaryDirectory(dir=path) as tempdir:
            zipfile = os.path.join(tempdir,file_obj.name)

            # download zip
            file_obj.download(zipfile)

            # use linux "unzip" methods in shell in case symbolic links exist
            log.info("Unzipping file, %s", os.path.basename(zipfile))
            cmd = ["unzip","-qq","-o",zipfile,"-d",tempdir]
            result = sp.run(cmd, check=True, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
            log.info("Done unzipping.")

            try:
                # use subprocess shell command here since there is a wildcard
                cmd = "mv "+os.path.join(tempdir,zip_top_dir,"*")+" "+path
                result = sp.run(cmd, shell=True, check=True, stdout=sp.PIPE, stderr=sp.PIPE)
            except sp.CalledProcessError as e:
                cmd = "cp -R "+os.path.join(tempdir,zip_top_dir,"*")+" "+path
                result = sp.run(cmd, shell=True, check=True, stdout=sp.PIPE, stderr=sp.PIPE)
            finally:
                cmd = ['rm','-Rf',os.path.join(tempdir,zip_top_dir)]
                run_command_with_retry(cmd, delay=5)
    else:
        path = os.path.join(path,"files")
        os.makedirs(path, exist_ok=True)
        zipfile = os.path.join(path,file_obj.name)
        # download zip
        file_obj.download(zipfile)
        # use linux "unzip" methods in shell in case symbolic links exist
        log.info("Unzipping file, %s", os.path.basename(zipfile))
        cmd = ["unzip","-qq","-o",zipfile,"-d",path]
        result = sp.run(cmd, check=True, stdout=sp.PIPE, stderr=sp.PIPE, text=True)
        log.info("Done unzipping.")
        os.remove(zipfile)
        

def run_command_with_retry(cmd, retries=3, delay=1, cwd=os.getcwd()):
    """Runs a command with retries and delay on failure."""

    for attempt in range(retries):
        try:
            result = sp.run(cmd, check=True, stdout=sp.PIPE, stderr=sp.PIPE, text=True, cwd=cwd)
            return result
        except sp.CalledProcessError as e:
            print(f"Attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                print(f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                raise  # Re-raise the exception if all retries fail
        
        
        
        
        
# def download_and_unzip_inputs(path, zip_filename):
#     """
#     unzip_inputs unzips the contents of zipped gear output into the working
#     directory.
#     Args:
#         zip_filename (string): The file to be unzipped
#     """
#     rc = 0
#     outpath = []
    
    
#     # if unzipped directory is a destination id - move all outputs one level up
#     with ZipFile(zip_filename, "r") as f:
#         top = [item.split('/')[0] for item in f.namelist()]

#     if len(top[0]) == 24:
        
#         # use linux "unzip" methods in shell in case symbolic links exist
#         log.info("Unzipping file, %s", zip_filename)
#         cmd = "unzip -qq -o " + zip_filename + " -d " + str(path)
#         terminal = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True, cwd=path)
#         log.info("Done unzipping.")

    
#         # directory starts with flywheel destination id - obscure this for now...

#         cmd = "mv " + top[0] + '/* . '
#         terminal = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True, cwd=path)
#         stdout, stderr = terminal.communicate()
#         if stderr:
#             cmd = "cp -R " + top[0] + '/* . '
#             terminal = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True, cwd=path)
#             stdout, stderr = terminal.communicate()
#             if stderr:
#                 log.debug("\n %s", stdout)
#                 log.error("\n %s", stderr)
#                 rc = terminal.poll()
#         cmd = 'rm -Rf ' + top[0] 
#         run_command_with_retry(cmd, cwd=path, delay=5)
# #         terminal = sp.Popen(cmd, shell=True, stdout=sp.PIPE, stderr=sp.PIPE, universal_newlines=True, cwd=path)
# #         stdout, stderr = terminal.communicate()

#     return rc


# def run_command_with_retry(cmd, retries=3, delay=1, cwd=os.getcwd()):
#     """Runs a command with retries and delay on failure."""

#     for attempt in range(retries):
#         try:
#             result = sp.run(cmd, check=True, stdout=sp.PIPE, stderr=sp.PIPE, text=True, cwd=cwd)
#             return result
#         except sp.CalledProcessError as e:
#             print(f"Attempt {attempt + 1} failed: {e}")
#             if attempt < retries - 1:
#                 print(f"Retrying in {delay} seconds...")
#                 time.sleep(delay)
#             else:
#                 raise  # Re-raise the exception if all retries fail
