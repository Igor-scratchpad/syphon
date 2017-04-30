#!/usr/bin/python3
"""
Syphon: download, rip, normalize gains and trim start/end silences from
        youtube playlist

    License
    -------
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""

from configparser import ConfigParser
from subprocess import Popen, PIPE
from multiprocessing.dummy import Pool as ThreadPool
from os import listdir
from shutil import copy2
import logging
import os
import sys


class Syphon():
    DEFAULT_GAIN = -12
    DEFAULT_THREADS = 4

    def initlogger(self, logfile="log.log", mainlevel=logging.DEBUG,
                   filelevel=logging.DEBUG, consolelevel=logging.DEBUG):
        '''initlogger'''
        # create logger
        logger = logging.getLogger()
        logger.setLevel(mainlevel)
        # create file handler which logs even debug messages
        fh = logging.FileHandler(logfile)
        fh.setLevel(filelevel)
        # create console handler also logging at DEBUG level
        ch = logging.StreamHandler()
        ch.setLevel(consolelevel)
        # create formatter and add it to the handlers
        formatter = logging.Formatter("%(asctime)s " +
                                      "[%(threadName)-12.12s] " +
                                      "[%(levelname)-5.5s]  %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

    def __init__(self):
        self.processed_files = 0
        self.gaindir = "./gain/"
        if not os.path.exists(self.gaindir):
            os.makedirs(self.gaindir)
        self.initlogger(logfile="syphon.log", consolelevel=logging.WARNING)
        # load configuration
        cfg = ConfigParser()
        try:
            cfg.read(os.path.dirname(sys.argv[0]) + "/syphon.ini")
            self.gain = int(cfg.get("GLOBAL", "TARGET_GAIN"))
            self.threads = int(cfg.get("GLOBAL", "MAX_THREADS"))
        except Exception:
            self.gain = self.DEFAULT_GAIN
            self.threads = self.DEFAULT_THREADS
        # load playlist URL
        urls = ConfigParser()
        try:
            urls.read(os.path.dirname(sys.argv[0]) + "/syphon_urls.ini")
            active = urls.get("URLS", "PLAYLIST_URL")
            self.playlist_url = urls.get("URLS", active)
            print(self.playlist_url)
        except Exception:
            exit(-1)

    def __logcommand(self, command=[]):
        '''__logcommand'''
        if not isinstance(command, list):
            return "", "", -1
        logging.info("Command:\n" + " ".join(command) + "\n")
        proc = Popen(command, stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = proc.communicate()
        output = output.decode("utf-8")
        err = err.decode("utf-8")
        logging.info("Output:\n" + output + "\n")
        logging.info("Error:\n" + err + "\n")
        logging.info("Return Code:\n" + str(proc.returncode) + "\n")
        return output, err, proc.returncode

    def downloadnewsongs(self):
        command = ['youtube-dl',
                   '-i',
                   '--download-archive', 'Archive.txt',
                   '--extract-audio', '--audio-format', 'vorbis',
                   '--keep-video', '-o',
                   '%(playlist_index)s-%(title)s.%(ext)s',
                   self.playlist_url]
        output, err, retcode = self.__logcommand(command)

    def __getgain(self, filename):
        '''__getgain'''
        logging.info("Extracting gain info.\n")
        command = ['normalize-ogg', '-n', filename]
        output, err, retcode = self.__logcommand(command)
        if "dBFS" in output:
            level = output.split()[0]
            logging.debug("Level: " + level)
            level = level.split("dBFS")[0]
            level = level.replace(',', '.')
        elif "ADJUST_NEEDED 0" in output:
            level = self.gain
        return int(round(float(level)))

    def __getbitrate(self, filename):
        '''__getbitrate'''
        logging.info("Extracting average bitrate.\n")
        command = ['exiftool', filename]
        output, err, retcode = self.__logcommand(command)
        bitrate = '0'
        for line in output.split('\n'):
            if 'Nominal Bitrate' in line:
                bitrate = line.split(':')[1].split()[0]
                break
        logging.info("Average bitrate is: " + str(bitrate) + "\n")
        return bitrate

    def __adjustgain(self, filename, delta, bitrate):
        filename = self.gaindir + filename
        '''__adjustgain'''
        logging.info("Re-normalizing.\n")
        command = ['normalize-ogg', '--ogg', '--bitrate', bitrate,
                   '-g', str(delta) + 'db', filename]
        output, err, retcode = self.__logcommand(command)
        if retcode:
            logging.critical("Re-normalizing failed.\n" +
                             "Output:\n" + output + "err:\n" + err)
            exit(retcode)

    def __normalizegain(self, filename):
        '''__normalizegain'''
        bitrate = self.__getbitrate(filename)
        if bitrate is '0':
            logging.error("No bitrate found, aborting conversion.\n")
            exit(-1)
        adjusted_file = "_" + filename
        copy2(filename, self.gaindir + adjusted_file)
        delta_gain = self.gain - self.__getgain(filename)
        if delta_gain is 0:
            logging.info(filename + " is already at the correct level")
        else:
            logging.info("Required adjustment: " + str(delta_gain) + "\n")
            self.__adjustgain(adjusted_file, delta_gain, bitrate)
        return adjusted_file

    def __trimstartsilence(self, src, dst):
        '''trimsilences'''
        logging.info("Trimming silences.\n")
        silences_from_start = "1"
        max_silence_duration = "120"
        silence_threshold = "2%"
        command = ["sox", src, dst,
                   "silence", silences_from_start, max_silence_duration,
                   silence_threshold]
        output, err, retcode = self.__logcommand(command)
        if retcode:
            logging.critical("Trimming failed.\n" +
                             "Output:\n" + output + "err:\n" + err)
            exit(retcode)

    def __reverse(self, src, dst):
        '''__reverse'''
        logging.info("Reversing.\n")
        command = ["sox", src, dst, "reverse"]
        output, err, retcode = self.__logcommand(command)
        if retcode:
            logging.critical("Reversing failed.\n" +
                             "Output:\n" + output +
                             "err:\n" + err)
            exit(retcode)

    def __trimsilences(self, filename):
        '''trimsilences'''
        self.__do_op_tmp(self.__trimstartsilence, filename)
        self.__do_op_tmp(self.__reverse, filename)
        self.__do_op_tmp(self.__trimstartsilence, filename)
        self.__do_op_tmp(self.__reverse, filename)

    def __do_op_tmp(self, op, filename):
        '''__do_op_tmp'''
        src = self.gaindir + filename
        dst = self.gaindir + "_" + filename
        op(src, dst)
        command = ["mv", dst, src]
        output, err, retcode = self.__logcommand(command)
        return retcode

    def __condition(self, filename):
        '''__condition'''
        logging.info("Conditioning " + filename)
        filename = self.__normalizegain(filename)
        self.__trimsilences(filename)
        command = ["mv", self.gaindir + filename,
                   self.gaindir + filename[1:]]
        output, err, retcode = self.__logcommand(command)
        return retcode

    def __parallelize(self, action, targets):
        '''parallelize'''
        pool = ThreadPool(self.threads)
        pool.map(action, targets)
        pool.close()
        pool.join()

    def parallelcondition(self):
        '''parallelcondition'''
        targets = [x for x in listdir(".")
                   if x.endswith("ogg") and x not in listdir(self.gaindir)]
        targets.sort()
        self.__parallelize(action=self.__condition, targets=targets)


if __name__ == "__main__":
    syphon = Syphon()
    syphon.downloadnewsongs()
    syphon.parallelcondition()
