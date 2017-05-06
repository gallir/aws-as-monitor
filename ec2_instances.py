#!/usr/bin/python2

import sys
import datetime
import argparse
from ec2_watchdata import WatchData


def main():
    global configuration

    WatchData.stats_period = 60  # Show just last measure
    data = WatchData(configuration.group)
    data.connect()
    data.get_instances_info()
    """ Check if we must change the desired instances """
    if configuration.instances > 1:
        desired = configuration.instances
        if desired > 0 and abs(data.instances - desired) < 3:
            data.set_desired(desired)
        else:
            print "You can specify up to +-2 instances more of currently running (%d)" % (
                data.instances, )
        exit(0)

    if configuration.kill:
        if configuration.kill in [x.id for x in data.instances_info]:
            data.kill_instance(configuration.kill, True)
        else:
            print "Instance", configuration.kill, "doesn't exist"

        exit(0)

    data.get_CPU_loads()

    print "Group values: instances: %d min: %d max: %d desired: %d Launch config: %s" % (
        data.instances, data.min_size, data.max_size,
        data.desired, data.group['LaunchConfigurationName'])

    for i, d in data.instances_info.iteritems():

        if i in data.loads:
            load = data.loads[i]
        else:
            load = 0

        print "%s %5.2f%% %s %s" % (i, load, d['State']['Name'], d['ImageId']),
        try:
            print "%s %s %-15s" % ( d['InstanceType'], d['Placement']['AvailabilityZone'],
                d['NetworkInterfaces'][0]['PrivateIpAddress']), 
        except:
          pass

        if configuration.all:
          try:
              print "%-15s" % (
                d['NetworkInterfaces'][0]['PrivateDnsName'])
          except:
              pass
        else:
            print

    print "Average load: %5.2f%%" % (data.avg_load, )

    if data.instances > 1:
        print "Average load with %d instances: %5.2f%%" % (
            data.instances - 1, data.total_load / (data.instances - 1))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--group", "-g", default="web", help="AutoScaler group")
    parser.add_argument(
        "--all",
        "-a",
        action="store_true",
        help="Show more info for every instance")

    group = parser.add_mutually_exclusive_group()
    group.add_argument("--kill", "-k", help="Kill instance")
    group.add_argument(
        "--instances",
        "-i",
        type=int,
        help="Set the number of desired instances")

    configuration = parser.parse_args()
    main()
