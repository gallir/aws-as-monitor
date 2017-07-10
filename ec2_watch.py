#!/usr/bin/python

import argparse
import sys
import time
import datetime
import getpass
import smtplib
from email.mime.text import MIMEText
import subprocess
import os

import boto
from ec2_watchdata import WatchData

LONG_PERIOD = 1080 # to check too low or low average
SHORT_PERIOD = 600 # to check averages
EMERGENCY_PERIOD = 120 # to check situations like very high in one instance


def main():
    global configuration

    now = int(time.time())
    data = WatchData(configuration.group)
    """ Set default class values """
    if configuration.dry:
        WatchData.dry = True
    if configuration.low:
        WatchData.low_limit = configuration.low
    if configuration.high:
        WatchData.high_limit = configuration.high
    if configuration.high_urgent:
        WatchData.high_urgent = configuration.high_urgent
    if configuration.history:
        WatchData.history_size = configuration.history
    if configuration.low_counter:
        WatchData.low_counter_limit = configuration.low_counter
    if configuration.high_counter:
        WatchData.high_counter_limit = configuration.high_counter
    if configuration.kill_counter:
        WatchData.kill_counter_limit = configuration.kill_counter

    try:
        data.connect()
        data.get_CPU_loads()
    except boto.exception.BotoServerError:
        print("Error in Boto")
        return

    prev_data = data.from_file()
    """ Retrieve and calculate previous values in the current instance """
    data.action_ts = int(prev_data.action_ts)
    data.action = prev_data.action
    data.up_ts = int(prev_data.up_ts)
    data.down_ts = int(prev_data.down_ts)
    data.history = prev_data.history

    try:
        data.low_counter = int(prev_data.low_counter)
    except AttributeError:
        data.low_counter = 0
    try:
        data.high_counter = int(prev_data.high_counter)
    except AttributeError:
        data.high_counter = 0

    """ Calculate the trend, increasing or decreasing CPU usage """
    alpha = min((data.ts - prev_data.ts) / 60.0 * 0.3, 1)
    data.exponential_average = alpha * data.avg_load + (
        1 - alpha) * prev_data.exponential_average
    data.trend = 2 * data.exponential_average - prev_data.exponential_average

    if data.instances != prev_data.instances:
        data.previous_instances = prev_data.instances
        if data.instances > prev_data.instances:
            data.up_ts = int(time.time())
        else:
            data.down_ts = int(time.time())
    else:
        data.previous_instances = prev_data.previous_instances

    if data.instances != prev_data.instances or data.desired != prev_data.desired:
        data.changed_ts = int(time.time())
    else:
        data.changed_ts = int(prev_data.changed_ts)

    print "%s values: instances: %d min: %d max: %d desired: %d" % (
        configuration.group, data.instances, data.min_size,
        data.max_size, data.desired)
    print "Average load: %5.2f%% Trend: %5.2f Max: %5.2f Min: %5.2f" % (data.avg_load, data.trend, data.max_load, data.min_load)
    if data.instances > 1:
        print "Average load with %d instances: %5.2f%%" % (
            data.instances - 1, data.total_load / (data.instances - 1))

    print "Last change: %s last action: %s (%s)" % (time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(data.changed_ts)), time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(data.action_ts)), data.action)
    print "Last up: %s last down: %s" % (time.strftime(
        '%Y-%m-%d %H:%M:%S', time.localtime(data.up_ts)), time.strftime(
            '%Y-%m-%d %H:%M:%S', time.localtime(data.down_ts)))

    if now - data.down_ts > LONG_PERIOD and now - data.action_ts > EMERGENCY_PERIOD and now - data.up_ts > SHORT_PERIOD:
        data.check_too_high()

    if now - data.changed_ts > LONG_PERIOD and now - data.action_ts > LONG_PERIOD:
        data.check_too_low()

    if now - data.changed_ts > SHORT_PERIOD and now - data.action_ts > SHORT_PERIOD:
        data.check_avg_high()

    if now - data.changed_ts > SHORT_PERIOD and now - data.action_ts > SHORT_PERIOD and now - data.up_ts > LONG_PERIOD:
        data.check_avg_low()

    data.store()

    if configuration.mail and data.emergency:
        sendmail(data, configuration.mail)


def sendmail(data, to):
    global configuration

    print "Sending email to", to
    """ Generate a report """
    try:
        p = subprocess.Popen(
            [
                os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "ec2_instances.py"),

                "-g", data.name,
                "-a"
            ],
            stdout=subprocess.PIPE)
        (report, err) = p.communicate()
    except Exception as e:
        report = unicode(e)

    msg = MIMEText("Action: " + data.action + "\n\n" + configuration.group + " INSTANCES SUMMARY:\n" +
                   unicode(report))
    msg['Subject'] = "Watch warning"
    msg['From'] = getpass.getuser()
    msg['To'] = configuration.mail
    s = smtplib.SMTP('localhost')
    s.sendmail(getpass.getuser(), configuration.mail, msg.as_string())
    s.quit()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--group", "-g", default="web", help="AutoScaler group")
    parser.add_argument(
        "--history",
        "-H",
        type=int,
        default=1800,
        help="History size of CPU load")
    parser.add_argument(
        "--mail",
        "-m",
        help="Send email to this address when took an emergency action")
    parser.add_argument(
        "--dry", "-d", action="store_true", help="Do not take actions")
    parser.add_argument(
        "--low",
        "-low",
        type=int,
        default=70,
        help="Low limit for CPU average")
    parser.add_argument(
        "--low_counter",
        type=int,
        default=5,
        help="Minimum times below low limit before reducing fleet")
    parser.add_argument(
        "--high_counter",
        type=int,
        default=3,
        help="Minimum times above limit before increasing fleet")
    parser.add_argument(
        "--kill_counter",
        type=int,
        default=15,
        help="Minimum times above limit before killing a bad instance")
    parser.add_argument(
        "--high",
        "-high",
        type=int,
        default=90,
        help="High limit for CPU average")
    parser.add_argument(
        "--high_urgent",
        "-u",
        type=int,
        default=95,
        help="Kill overloaded instance, or increase instances at this individual CPU load"
    )
    configuration = parser.parse_args()
    main()
