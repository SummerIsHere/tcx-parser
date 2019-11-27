#! python3



import os, pandas as pd, logging, glob, argparse
from lxml import etree
## Setup logging

## Set up logging file
logging.basicConfig(level=logging.INFO
                    #, filename='prase_tcx_logging.txt'
                    , format=' %(asctime)s - %(name)s - %(levelname)s - %(message)s')


## Setup argparse

import argparse

parser = argparse.ArgumentParser(description='Convert a folder of .tcx files to a .csv. Easiest behavior is to copy this .py file into the directory of .tcx files and running without arguments.')
parser.add_argument('--folder', '-f', help='Path to folder to scan for .tcx files. Default if omitted is current working directory. Use single quotes to denote pathname and do not use a slash at the end', default=os.getcwd())
parser.add_argument('--output', '-o', help='Name of .csv to write to. Follows same rules as above for specifying a full path. Default if omitted is output.csv',default='output.csv')
parser.add_argument('--keep', '-k', help='Include flag if you want to keep existing output data',default=False,action='store_true')

args = parser.parse_args()


def process_trackpoint(trackpoint,ns1,lap_start,df):
    temp_ts = None
    temp_hr = None
    temp_lat = None
    temp_long = None
    temp_alt = None
    temp_dist = None
    for elem in trackpoint.iter():
        if elem.tag == '{%s}Time'%ns1:
            temp_ts = elem.text
        elif elem.tag == '{%s}AltitudeMeters'%ns1:
            temp_alt = elem.text
        elif elem.tag == '{%s}DistanceMeters'%ns1:
            temp_dist = elem.text
        elif elem.tag == '{%s}HeartRateBpm'%ns1:
            for subelem in elem.iter():
                if subelem.tag == '{%s}Value'%ns1:
                    temp_hr = subelem.text
        elif elem.tag == '{%s}Position'%ns1:
            for subelem in elem.iter():
                if subelem.tag == '{%s}LatitudeDegrees'%ns1:
                    temp_lat = subelem.text
                elif subelem.tag == '{%s}LongitudeDegrees'%ns1:
                    temp_long = subelem.text
    temp_d = {  'Timestamp': [temp_ts], 'HeartRateBpm': [temp_hr]
                ,'Latitude':[temp_lat], 'Longitude':[temp_long], 'AltitudeMeters':[temp_alt]
                ,'DistanceMeters':[temp_dist], 'LapStartTime':[lap_start]
                }
    temp_df = pd.DataFrame(data=temp_d)
    df = df.append(temp_df)
    return df

def process_tcx_file(tcx_file):
    ns1 = 'http://www.garmin.com/xmlschemas/TrainingCenterDatabase/v2'
    tree = etree.parse(tcx_file)
    root = tree.getroot()
    dd = {'Timestamp': [None], 'HeartRateBpm': [None], 'Latitude': [None]
        ,'Longitude': [None],  'AltitudeMeters':[None], 'DistanceMeters':[None]
        ,'LapStartTime':[None],'Activity': [None],'tcx_file': [None]}
    tcx_file_df = pd.DataFrame(data=dd)
    blank_tcx_df = tcx_file_df.copy(deep=True)
    for element in root.iter():
        activity = None
        sport = None
        if element.tag == '{%s}Activity' % ns1:
            tcx_df = blank_tcx_df.copy(deep=True)
            sport = element.get('Sport')
            for subact in element.iter():
                if subact.tag == '{%s}Lap'%ns1:
                    logging.debug('subact.tag: ' + str(subact.tag))
                    lap_ts = subact.get('StartTime')
                    logging.debug('lap_ts: ' + str(lap_ts))
                    for sublap in subact.iter():
                        if sublap.tag == '{%s}Trackpoint'%ns1:
                            #logging.debug('sublap.tag: ' + str(sublap.tag))
                            tcx_df = process_trackpoint(sublap,ns1,lap_ts,tcx_df)
                elif subact.tag == '{%s}Plan'%ns1:
                    #logging.debug('subact.tag: ' + str(subact.tag))
                    for subplan in subact.iter():
                        if subplan.tag == '{%s}Name'%ns1:
                            #logging.debug('subplan.tag: ' + str(subplan.tag))
                            activity = subplan.text
            tcx_df.loc[:, 'Activity'] = activity
            tcx_df.loc[:, 'Sport'] = sport
            tcx_file_df = tcx_file_df.append(tcx_df)
    tcx_file_df.loc[:, 'tcx_file'] = tcx_file
    logging.debug(str(tcx_file_df))
    return tcx_file_df


def process_folder(folder_path=os.getcwd(), keep_existing=False, output_csv='polar_flow.csv'):
    logging.info('Scanning ' + str(folder_path) + ' for .tcx files')
    fl = glob.glob(os.path.join(folder_path, '*.tcx'))
    tcx_table = None
    loaded_fl = None
    if keep_existing:
        logging.info('Loading existing data from ' + output_csv)
        tcx_table = pd.read_csv(output_csv)
        loaded_fl = tcx_table.loc[:,'tcx_file'].unique()
        loaded_fl = loaded_fl.tolist()
        if len(loaded_fl) > 0:
            logging.info('Remove already loaded files from file list')
            fl = list(set(fl) - set(loaded_fl))
    for this_tcx in fl:
        logging.info('Processing ' + this_tcx)
        temp = process_tcx_file(this_tcx)
        tcx_table = temp.append(tcx_table)
    tcx_table.dropna(subset=['Timestamp'],inplace=True)
    logging.info('Writing .csv to ' + output_csv)
    tcx_table.to_csv(output_csv, index=False)
    return tcx_table

process_folder(folder_path=args.folder, keep_existing=args.keep, output_csv=args.output)
logging.info('Done!')
