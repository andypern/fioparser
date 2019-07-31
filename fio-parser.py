#!/usr/bin/env python
import argparse
import json
import os
import sys
import traceback
import re


"""
Analyze fio output

This is an attempt to work with both old FIO, as well as 'new' FIO. json output.

"""

class Ddict(dict):
    def __init__(self, default=None):
        self.default = default

    def __getitem__(self, key):
        if not self.has_key(key):
            self[key] = self.default()
        return dict.__getitem__(self, key)



def get_output_files(dir_path):
    file_list = []

    for f in os.listdir(dir_path):
        file_path = os.path.join(dir_path, f)

        if (not f.startswith('.') and os.path.isfile(file_path) and
                not f.endswith('.bak')):
            file_list.append(file_path)

    return file_list




def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-d', "--output-dir", required=True,
                        help="The absolute directory path containing fio "
                             "output files.")
    parser.add_argument('-m', "--metrics", nargs='*',
                        default=['read.iops','write.iops'],
                        help="The metrics definition.")
    parser.add_argument('-j', "--jobs", default=False, help="print each job metrics separately, otherwise sum/average.")
    parser.add_argument('-p', "--percentile", default=['99.900000'], help="which percentile granularities to display", nargs='*')
    parser.add_argument('-o', "--outputformat", default='csv', help="output is either csv or json for now")



    args = parser.parse_args()

    output_files = get_output_files(args.output_dir)


    output_hash = {}

    for output_file in output_files:
        oFile = os.path.basename(output_file)




        #print('Processing file: %s' % output_file)
        output_hash[oFile] = Ddict( dict )
        output_hash[oFile]['clientCount'] = 0

        try:
            with open(output_file, 'r') as f:
                output = json.load(f)
        except Exception as e:
            print('Error occured when open the output file %s, error: %s' %
                  (output_file, str(e)))
            continue

        output_hash[oFile]['globalOpts'] = output['global options']
        #print json.dumps(output_hash[oFile])


        #big hack...
        reMatch = re.match(r"[0-9]+\.[0-9a-z]+\.[0-9a-z]+\.[0-9a-z]+\.[0-9a-z]+\.([0-9]+)[m]\.([0-9]+)[t]\.", oFile)
        if reMatch:
            output_hash[oFile]['globalOpts']['mntPerClient'] = reMatch.group(1)
            output_hash[oFile]['globalOpts']['threadPerClient'] = reMatch.group(2)
        else:
            meMatch = re.match(r".+\.([0-9]+)[m]\.([0-9]+)[t]\.", oFile)
            if meMatch:
                output_hash[oFile]['globalOpts']['mntPerClient'] = meMatch.group(1)
                output_hash[oFile]['globalOpts']['threadPerClient'] = meMatch.group(2)


        #stuff we care about for now


        output_hash[oFile]['version'] = output['fio version']
        if output['fio version'] in "fio-2.2.8":
            lat = "clat"
            jobs = output['jobs']

        elif output['fio version'] in "fio-3.1":
            lat = "clat_ns"
            jobs = output['client_stats']


        try:
            #
            #build out sum/avearage stuff
            #


            for metric in args.metrics:
                keys = metric.split('.')
                for k in keys:
                    if ',' in k:
                        k = k.replace(',', '.')
                output_hash[oFile][metric]['sum'] = 0

            #
            #Latency
            #

            # first: means.

            for dir in ["write","read"]:
                clatMean = dir + "." + lat + "." + "mean"
                output_hash[oFile][clatMean]['sum'] = 0
            #
            #do so for percentile stuff
            #
            for pct in args.percentile:
                direction = ["write","read"]
                for dir in direction:
                    dirpct = dir + "." + lat + "." + "percentile." + pct
                    output_hash[oFile][dirpct]['sum'] = 0



            for j in jobs:
                output_hash[oFile]['clientCount'] += 1


                data = {}
                job_name = j['jobname']

                for m in args.metrics:
                    attribute = j
                    keys = m.split('.')
                    for k in keys:
                        if ',' in k:
                            k = k.replace(',', '.')
                        attribute = attribute.get(k)

                    data_key = '%s.%s' % (job_name, m)
                    data[data_key] = attribute
                    output_hash[oFile][m]['sum'] += attribute

                for dir in ["write","read"]:
                    attribute = j
                    clatMean = dir + "." + lat + "." + "mean"
                    try:
                        output_hash[oFile][clatMean]['sum'] += attribute[dir][lat]['mean']
                    except:
                        mydir = False

                for pct in args.percentile:
                    direction = ["write","read"]
                    attribute = j
                    #have to do for reads and writes
                    for dir in direction:
                        dirpct = dir + "." + lat + "." + "percentile." + pct
                        try:
                            output_hash[oFile][dirpct]['sum'] += attribute[dir][lat]['percentile'][pct]
                            #output_hash[output_file][dirpct]['sum'] += attribute[dirpct]

                        except:
                            mydir = False


            #calc averages
            direction = ["write","read"]
            for dir in direction:
                ops = dir + '.iops'
                output_hash[oFile][ops]['avg'] = int(output_hash[oFile][ops]['sum']) / int(output_hash[oFile]['clientCount'])
                clatMean = dir + "." + lat + "." + "mean"
                output_hash[oFile][clatMean]['avg'] = (float(output_hash[oFile][clatMean]['sum']) / int(output_hash[oFile]['clientCount']) / 1000000)
                for pct in args.percentile:
                        dirpct = dir + "." + lat + "." + "percentile." + pct
                        output_hash[oFile][dirpct]['avg'] = (float(output_hash[oFile][dirpct]['sum'])  / int(output_hash[oFile]['clientCount']) / 1000000)
            #
            #calc total threads / target ops
            #
            #   print output_hash[oFile]['globalOpts']['rate_iops']
            output_hash[oFile]['totThreads'] = int(output_hash[oFile]['clientCount']) * int(output_hash[oFile]['globalOpts']['threadPerClient'])
            output_hash[oFile]['targetTotal'] = int(output_hash[oFile]['globalOpts']['rate_iops']) * int(output_hash[oFile]['totThreads'])
            #print "%s , %s, %s" %(output_hash[oFile]['globalOpts']['rate_iops'], output_hash[oFile]['totThreads'],str(output_hash[oFile]['targetTotal']))

            #print "%s , %s , %s" % (output_hash[oFile]['clientCount'], output_hash[oFile]['globalOpts']['threadPerClient'], output_hash[oFile]['totThreads'])
        except Exception as e:
            print(''
                  'fname: %s error: %s' %(oFile, str(e)))
            exc_type, exc_value, exc_traceback = sys.exc_info()
            traceback.print_exception(exc_type, exc_value, exc_traceback,
                                      limit=4, file=sys.stdout)
            continue



    if args.outputformat in 'json':
        print(json.dumps(output_hash, indent=2))
    elif args.outputformat in 'csv':
        print 'fName,bs,rwmix,depth,clients,totThreads,target,r-ops,r-mean,r-99.9,w-ops,w-mean,w-99.9,totOps'
        for output_file in output_files:
            oFile = os.path.basename(output_file)
            oHash = output_hash[oFile]
            opts = oHash['globalOpts']
            oHash['totOps'] = 0


            csv_output = oFile + ',' + opts['bs'] + ',' + opts['rwmixread'] + ','
            csv_output += str(opts['iodepth']) + ',' + str(oHash['clientCount']) + ','
            csv_output += str(oHash['totThreads']) + ',' + str(oHash['targetTotal']) + ','

            try:
                csv_output += str(oHash['read.iops']['sum']) + ',' + str(oHash['read.clat_ns.mean']['avg']) + ','
                oHash['totOps'] += int(oHash['read.iops']['sum'])
            except KeyError as e:
                csv_output += 'n/a,n/a,'

            if output_hash[oFile]['read.iops']['sum'] > 0:
                csv_output += str(output_hash[oFile]['read.clat_ns.percentile.99.900000']['avg']) + ','
            else:
                csv_output += 'n/a,'
            try:
                csv_output += str(oHash['write.iops']['sum']) + ',' + str(oHash['write.clat_ns.mean']['avg']) + ','
                oHash['totOps'] += int(oHash['write.iops']['sum'])

            except KeyError as e:
                csv_output += 'n/a,n/a,'

            if output_hash[oFile]['write.iops']['sum'] > 0:
                try:
                    csv_output += str(output_hash[oFile]['write.clat_ns.percentile.99.900000']['avg']) + ','
                except KeyError as e:
                    print "%s , %s" %(e,oFile)
            else:
                csv_output += 'n/a,'

            csv_output += str(oHash['totOps'])


            print csv_output




if __name__ == '__main__':
    main()
