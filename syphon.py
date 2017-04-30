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
import os
from shutil import copyfile
from shutil import copy2
from shutil import rmtree
import logging
from sqlite3 import dbapi2 as sqlite
import pickle
from mutagen.oggvorbis import OggVorbis
import acoustid

CFG_PATH = "/usr/share/syphon"


class Syphon():
    '''Syphon'''
    @classmethod
    def __preparepaths(cls, cfgpath, basepath):
        '''__preparepaths'''
        cls.__paths = {}
        cls.__paths["cfg"] = cfgpath
        cls.__paths["basepath"] = basepath
        cls.__paths["downloads"] = os.path.join(basepath, "downloads")
        cls.__paths["normalized"] = os.path.join(basepath, "normalized")
        cls.__paths["pool"] = os.path.join(basepath, "pool")
        cls.__paths["mp3"] = os.path.join(basepath, "mp3")
        cls.__paths["pls"] = os.path.join(basepath, "playlists")
        cls.__paths["custom"] = os.path.join(basepath, "custom")
        cls.__paths["devices"] = os.path.join(basepath, "devices")

    @classmethod
    def __inpath(cls, path, filename):
        '''__inpath'''
        return os.path.join(cls.__paths[path], filename)

    @classmethod
    def __loadbaseconfig(cls):
        '''__loadbaseconfig'''
        try:
            parser = ConfigParser()
            cfgfile = os.path.join(CFG_PATH, "syphon.ini")
            parser.read(cfgfile)
            cls.__gain = parser.getint("GLOBAL", "TARGET_GAIN")
            cls.threads = parser.getint("GLOBAL", "MAX_THREADS")
            basepath = os.path.expanduser(parser.get("GLOBAL", "BASE_PATH"))
            cls.__preparepaths(CFG_PATH, basepath)
            return True
        except Exception:
            print("Error while parsing " + cfgfile)
            return False

    @classmethod
    def __loadkeyconfig(cls):
        '''__loadkeyconfig'''
        try:
            parser = ConfigParser()
            cfgfile = cls.__inpath("cfg", "syphon_key.ini")
            parser.read(cfgfile)
            cls.key = parser.get("KEY", "VALUE")
            return True
        except Exception:
            print("Error while parsing " + cfgfile)
            return False

    @classmethod
    def __loadplaylistsconfig(cls):
        '''__loadplaylistsconfig'''
        try:
            parser = ConfigParser()
            cfgfile = cls.__inpath("cfg", "syphon_urls.ini")
            parser.read(cfgfile)
            cls.__playlists = []
            for section in parser.sections():
                if parser.getboolean(section, "ACTIVE"):
                    cls.__playlists.append({
                        "name": section,
                        "type": "auto",
                        "url": parser.get(section, "URL"),
                        "path": cls.__inpath("downloads", section),
                        })
            return True
        except Exception:
            print("Error while parsing " + cfgfile)
            return False

    @classmethod
    def __loaddevicesconfig(cls):
        '''__loaddevicesconfig'''
        try:
            parser = ConfigParser()
            cfgfile = cls.__inpath("cfg", "syphon_devices.ini")
            parser.read(cfgfile)
            cls.__devices = []
            for section in parser.sections():
                cls.__devices.append({
                    "name": section.lower(),
                    "playlists": parser.get(section, "PLAYLISTS").split()
                    })
            return True
        except Exception:
            print("Error while parsing " + cfgfile)
            return False

    @classmethod
    def __loadconfigs(cls):
        '''__loadconfigs'''
        return (cls.__loadbaseconfig() and
                cls.__loadkeyconfig() and
                cls.__loadplaylistsconfig() and
                cls.__loaddevicesconfig() and
                True)

    @classmethod
    def __initlogger(cls, logfile="log.log", mainlevel=logging.DEBUG,
                     filelevel=logging.DEBUG, consolelevel=logging.DEBUG):
        '''__initlogger'''
        # create logger
        logger = logging.getLogger()
        logger.setLevel(mainlevel)
        # create file handler which logs even debug messages
        fh = logging.FileHandler(cls.__inpath("basepath", logfile))
        fh.setLevel(filelevel)
        # create console handler also logging at DEBUG level
        ch = logging.StreamHandler()
        ch.setLevel(consolelevel)
        # create formatter and add it to the handlers
        formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] " +
                                      "[%(levelname)-5.5s]  %(message)s")
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        # add the handlers to the logger
        logger.addHandler(fh)
        logger.addHandler(ch)

    @classmethod
    def __init__(cls):
        if not cls.__loadconfigs():
            exit(-1)
        cls.__dbfile = cls.__inpath("cfg", "syphon.db")
        cls.__processed_files = 0
        cls.__con = 0
        cls.__found = False
        cls.acoustids = 0
        cls.__initlogger(logfile="syphon.log", consolelevel=logging.WARNING)

    @classmethod
    def __logcommand(cls, command=[""]):
        '''__logcommand'''
        if not isinstance(command, list) or command == [""]:
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

    @classmethod
    def __downloadnewsongs(cls):
        '''__downloadnewsongs'''
        c = ['youtube-dl', '-i', '--download-archive', 'Archive.txt',
             '--extract-audio', '--audio-format', 'vorbis', '--keep-video',
             '-o', '%(playlist_index)s-%(title)s.%(ext)s',
             cls.__playlist["url"]]
        output, err, retcode = cls.__logcommand(c)

    @classmethod
    def __getgain(cls, filename):
        '''__getgain'''
        logging.info("Extracting gain info.\n")
        command = ['normalize-ogg', '-n', filename]
        output, err, retcode = cls.__logcommand(command)
        if "dBFS" in output:
            level = output.split()[0]
            logging.debug("Level: " + level)
            level = level.split("dBFS")[0]
            level = level.replace(',', '.')
        elif "ADJUST_NEEDED 0" in output:
            level = cls.__gain
        return int(round(float(level)))

    @classmethod
    def __getbitrate(cls, filename):
        '''__getbitrate'''
        logging.info("Extracting average bitrate.\n")
        command = ['exiftool', filename]
        output, err, retcode = cls.__logcommand(command)
        bitrate = 0
        for line in output.split('\n'):
            if 'Nominal Bitrate' in line:
                bitrate = line.split(':')[1].split()[0]
                break
        logging.info("Average bitrate is: " + str(bitrate) + "\n")
        return bitrate

    @classmethod
    def __adjustgain(cls, filename, delta, bitrate):
        '''__adjustgain'''
        filename = cls.__inpath("normalized", filename)
        logging.info("Re-normalizing.\n")
        command = ['normalize-ogg', '--ogg', '--bitrate', bitrate,
                   '-g', str(delta) + 'db', filename]
        output, err, retcode = cls.__logcommand(command)
        if retcode:
            logging.critical("Re-normalizing failed.\n" +
                             "Output:\n" + output + "err:\n" + err)
            exit(retcode)

    @classmethod
    def __normalizegain(cls, filename):
        '''__normalizegain'''
        bitrate = cls.__getbitrate(filename)
        if bitrate is 0:
            logging.error("No bitrate found, aborting conversion.\n")
            exit(-1)
        adjusted_file = "_" + filename
        copy2(filename, cls.__inpath("normalized", adjusted_file))
        delta_gain = cls.__gain - cls.__getgain(filename)
        if delta_gain is 0:
            logging.info(filename + " is already at the correct level")
        else:
            logging.info("Required adjustment: " + str(delta_gain) + "\n")
            cls.__adjustgain(adjusted_file, delta_gain, bitrate)
        return adjusted_file

    @classmethod
    def __trimstartsilence(cls, src, dst):
        '''trimsilences'''
        logging.info("Trimming silences.\n")
        silences_from_start = "1"
        max_silence_duration = "120"
        silence_threshold = "2%"
        command = ["sox", src, dst,
                   "silence", silences_from_start, max_silence_duration,
                   silence_threshold]
        output, err, retcode = cls.__logcommand(command)
        if retcode:
            logging.critical("Trimming failed.\n" +
                             "Output:\n" + output + "err:\n" + err)
            exit(retcode)

    @classmethod
    def __reverse(cls, src, dst):
        '''__reverse'''
        logging.info("Reversing.\n")
        command = ["sox", src, dst, "reverse"]
        output, err, retcode = cls.__logcommand(command)
        if retcode:
            logging.critical("Reversing failed.\n" +
                             "Output:\n" + output +
                             "err:\n" + err)
            exit(retcode)

    @classmethod
    def __do_op_tmp(cls, op, filename):
        '''__do_op_tmp'''
        src = cls.__inpath("normalized", filename)
        dst = cls.__inpath("normalized", "_" + filename)
        op(src, dst)
        command = ["mv", dst, src]
        output, err, retcode = cls.__logcommand(command)
        return retcode

    @classmethod
    def __trimsilences(cls, filename):
        '''trimsilences'''
        cls.__do_op_tmp(cls.__trimstartsilence, filename)
        cls.__do_op_tmp(cls.__reverse, filename)
        cls.__do_op_tmp(cls.__trimstartsilence, filename)
        cls.__do_op_tmp(cls.__reverse, filename)

    @classmethod
    def __condition(cls, filename):
        '''__condition'''
        logging.info("Conditioning " + filename)
        filename = cls.__normalizegain(filename)
        cls.__trimsilences(filename)
        cmd = ["mv", cls.__inpath("normalized", filename),
               cls.__inpath("normalized", filename[1:])]
        output, err, retcode = cls.__logcommand(cmd)
        return retcode

    @classmethod
    def __addsongtodb(cls, filename):
        '''__addsongtodb'''
        logging.info("Adding song to DB " + filename)
        full_path_filename = cls.__inpath("normalized", filename)
        fingerprint = acoustid.fingerprint_file(full_path_filename)
        pickled = pickle.dumps(fingerprint)
        con = sqlite.connect(cls.__dbfile)
        cursor = con.cursor()
        try:
            cursor.execute('INSERT INTO ' +
                           'Songs("Input File Name", AcoustID) ' +
                           'VALUES(?, ?)''', (filename, pickled,))
            con.commit()
        except Exception:
            logging.info(filename + " already present")

    @classmethod
    def __convert(cls, filename):
        '''__convert'''
        logging.info("Converting " + filename)
        outfile = cls.__inpath("mp3", filename[:-3] + "mp3")
        if not os.path.exists(outfile):
            infile = cls.__inpath("pool", filename)
            cmd = ["ffmpeg", "-i", infile, "-map_metadata", "0:s:0",
                   "-q:a", "6", outfile]
            output, err, retcode = cls.__logcommand(cmd)
            return retcode
        return 0

    @classmethod
    def __parallelize(cls, action, targets):
        '''__parallelize'''
        pool = ThreadPool(cls.threads)
        pool.map(action, targets)
        pool.close()
        pool.join()

    @classmethod
    def __reindex(cls):
        '''__reindex'''
        for src in os.listdir("."):
            if src == 'Archive.txt' or src == '.directory':
                continue
            pos = src.find('-')
            if pos >= 3:
                continue
            if pos is 1:
                prefix = '00'
            else:
                prefix = '0'
            dst = prefix + src
            os.rename(src, dst)

    @classmethod
    def __parallelcondition(cls):
        '''__parallelcondition'''
        targets = [x for x in os.listdir(".")
                   if (x.endswith("ogg") and
                       x not in os.listdir(cls.__paths["normalized"]))]
        targets.sort()
        cls.__parallelize(action=cls.__condition, targets=targets)

    @classmethod
    def __parallelconvert(cls):
        '''__parallelconvert'''
        targets = [x for x in os.listdir(cls.__paths["pool"])
                   if (x.endswith("ogg"))]
        targets.sort()
        cls.__parallelize(action=cls.__convert, targets=targets)

    @classmethod
    def __getrawplaylist(cls):
        playlist = [x for x in os.listdir(".") if x.endswith("ogg")]
        playlist.sort()
        cls.__playlist["rawplaylist"] = playlist

    @classmethod
    def __loadsongsdb(cls):
        '''__loadsongsdb'''
        con = sqlite.connect(cls.__dbfile)
        cursor = con.cursor()
        cursor.execute('SELECT "Input File Name", Title, Artists'
                       ' FROM Songs')
        cls.songslist = [{"in": x[0], "title": x[1], "artists": x[2]}
                         for x in cursor.fetchall()]

    @classmethod
    def __paralleladdsongtodb(cls):
        '''__paralleladdsongtodb'''
        cls.__loadsongsdb()
        filenames = [x["in"] for x in cls.songslist]
        filenames.sort()
        targets = [x for x in os.listdir(cls.__paths["normalized"])
                   if x.endswith("ogg") and x not in filenames]
        targets.sort()
        cls.__parallelize(action=cls.__addsongtodb, targets=targets)

    @classmethod
    def __extractuniquenotnulltitles(cls):
        '''__extractuniquenotnulltitles'''
        con = sqlite.connect(cls.__dbfile)
        cursor = con.cursor()
        columns = 'Title, Artists, "Input File Name"'
        cursor.execute('SELECT ' + columns + ' FROM Songs ' +
                       'WHERE Title is not NULL and Artists is not NULL ' +
                       'order by ' + columns)
        raw_targets = [{'title': x[0], 'artists': x[1], 'in': x[2]}
                       for x in cursor.fetchall()]
        i = 0
        j = 0
        targets = []
        targets.append(raw_targets[j])
        for j in range(len(raw_targets)):
            if (targets[i]["title"] != raw_targets[j]["title"]) or \
               (targets[i]["artists"] != raw_targets[j]["artists"]):
                targets.append(raw_targets[j])
                i += 1
        return targets

    @classmethod
    def __assemblename(cls, title, artists, ext):
        '''__assemblename'''
        return title + " _ " + artists + "." + ext

    @classmethod
    def __assembleoggname(cls, title, artists):
        '''__assembleoggname'''
        return cls.__assemblename(title=title, artists=artists, ext="ogg")

    @classmethod
    def __assemblemp3name(cls, title, artists):
        '''__assemblemp3name'''
        return cls.__assemblename(title=title, artists=artists, ext="mp3")

    @classmethod
    def __copyandtag(cls, target):
        '''__copyandtag'''
        src = cls.__inpath("normalized", target['in'])
        out = cls.__assembleoggname(target['title'], target['artists'])
        dst = cls.__inpath("pool", out)
        if not os.path.exists(src):
            return
        if not os.path.exists(dst):
            try:
                copyfile(src, dst)
            except Exception:
                logging.info("Failed copying " + src + " to " + dst)
                return
        try:
            ogg = OggVorbis(dst)
            if ogg.get("title", None) is None or \
               ogg.get("artist", None) is None or \
               ogg["title"] != target["title"] or \
               ogg["artist"] != target["artists"]:
                ogg["title"] = target["title"]
                ogg["artist"] = target["artists"]
                ogg.save(dst)
        except Exception:
            logging.info("Failed tagging " + dst)

    @classmethod
    def __parallelcopyandtag(cls):
        '''__parallelcopyandtag'''
        targets = cls.__extractuniquenotnulltitles()
        cls.__parallelize(action=cls.__copyandtag, targets=targets)

    @classmethod
    def __refinerawplaylist(cls, rawplaylist):
        '''__refinerawplaylist'''
        playlist = []
        for filename in rawplaylist:
            entry = [x for x in cls.songslist
                     if (x["in"] == filename and x["title"] is not None and
                         x["artists"] is not None)]
            if len(entry) > 0:
                out = cls.__assemblemp3name(entry[0]["title"],
                                            entry[0]["artists"])
                playlist.append(out)
        return playlist

    @classmethod
    def __storeplaylisttofile(cls, target):
        '''__storeplaylisttofile'''
        plsfile = cls.__inpath("pls", target["name"] + ".m3u")
        with open(plsfile, "w") as dst:
            dst.writelines(["mp3/" + x + "\n"
                            for x in target["playlist"]])

    @classmethod
    def __storeplaylisttodb(cls, name, plstype, playlist):
        '''__storeplaylisttodb'''
        logging.info("Adding playlist to DB " + name)
        playlist = str(playlist)
        try:
            con = sqlite.connect(cls.__dbfile)
            cursor = con.cursor()
            cursor.execute('SELECT Songs from Playlists ' +
                           'WHERE Playlist == ?', (name,))
            oldplaylist = cursor.fetchall()
            if oldplaylist != [] and oldplaylist[0][0] == playlist:
                return
            con = sqlite.connect(cls.__dbfile)
            cursor = con.cursor()
            if oldplaylist == []:
                cursor.execute('INSERT INTO ' +
                               'Playlists(Playlist, Type, Songs) ' +
                               'VALUES(?, ?, ?)',
                               (name, plstype, playlist,))
            else:
                cursor.execute('UPDATE Playlists ' +
                               'SET Songs = ?, Type = ? ' +
                               'WHERE Playlist == ?',
                               (playlist, plstype, name))
            con.commit()
        except Exception:
            logging.info("error storing playlist " + name)
            return

    @classmethod
    def __updateautoplaylist(cls, target):
        '''__updateautoplaylist'''
        rawplaylist = target["rawplaylist"]
        target["playlist"] = cls.__refinerawplaylist(rawplaylist)
        cls.__storeplaylisttofile(target)
        cls.__storeplaylisttodb(target["name"], target["type"],
                                target["playlist"])

    @classmethod
    def __parallelupdateautoplaylist(cls):
        '''__parallelupdateautoplaylist'''
        cls.__loadsongsdb()
        targets = [x for x in cls.__playlists if x["type"] == "auto"]
        cls.__parallelize(action=cls.__updateautoplaylist, targets=targets)

    @classmethod
    def __loadcustomplaylists(cls):
        '''__loadcustomplaylist'''
        filenames = [x for x in os.listdir(cls.__paths["custom"])
                     if x.endswith(".m3u")]
        for filename in filenames:
            with open(cls.__inpath("custom", filename)) as infile:
                lines = [l.split('/')[-1][:-4] + "mp3"
                         for l in infile.readlines()
                         if not l.startswith("#")]
            cls.__playlists.append({
                "name": filename[:-4],
                "type": "custom",
                "playlist": [str(l) for l in lines]
                })

    @classmethod
    def __updatecustomplaylist(cls, target):
        '''__updatecustomplaylist'''
        cls.__storeplaylisttofile(target)
        cls.__storeplaylisttodb(target["name"], target["type"],
                                target["playlist"])

    @classmethod
    def __parallelupdatecustomplaylist(cls):
        '''__parallelupdatecustomplaylist'''
        cls.__loadcustomplaylists()
        targets = [x for x in cls.__playlists if x["type"] == "custom"]
        cls.__parallelize(action=cls.__updatecustomplaylist, targets=targets)

    @classmethod
    def __preparepath(cls, path):
        '''__preparepath'''
        try:
            if not os.path.exists(path):
                os.mkdir(path)
        except Exception:
            print("Error while preparing " + path)
            exit(-1)

    @classmethod
    def __preparebasepaths(cls):
        '''__preparebasepath'''
        paths = [cls.__paths["basepath"], cls.__paths["downloads"],
                 cls.__paths["normalized"], cls.__paths["pool"],
                 cls.__paths["mp3"], cls.__paths["pls"],
                 cls.__paths["custom"], cls.__paths["devices"], ]
        for path in paths:
            cls.__preparepath(path)

    @classmethod
    def __updateytplaylist(cls):
        '''__updateytplaylist'''
        cls.__preparepath(cls.__playlist["path"])
        os.chdir(cls.__playlist["path"])
        cls.__downloadnewsongs()
        cls.__reindex()
        cls.__parallelcondition()
        cls.__getrawplaylist()

    @classmethod
    def __createpath(cls, path):
        '''__createpath'''
        if os.path.exists(path) and os.path.isfile(path):
            os.remove(path)
        if not os.path.exists(path):
            os.mkdir(path)

    @classmethod
    def __updatedevice(cls, device):
        '''__updatedevice'''
        devicepath = cls.__inpath("devices", device["name"])
        cls.__createpath(devicepath)
        mp3path = os.path.join(devicepath, "mp3")
        cls.__createpath(mp3path)
        for entry in os.listdir(devicepath):
            fullentry = os.path.join(devicepath, entry)
            if os.path.isfile(fullentry):
                os.remove(fullentry)
            else:
                if entry != "mp3":
                    rmtree(fullentry)
        mp3list = []
        for p in device["playlists"]:
            srcpl = cls.__inpath("pls", p + ".m3u")
            if os.path.exists(srcpl):
                dstpl = os.path.join(devicepath, p + ".m3u")
                copyfile(srcpl, dstpl)
                mp3list.extend([pl for pl in cls.__playlists
                                if pl["name"] == p][0]["playlist"])
        curmp3 = os.listdir(mp3path)
        for entry in curmp3:
            if entry not in mp3list:
                fullentry = os.path.join(mp3path, entry)
                if os.path.isfile(fullentry):
                    os.remove(fullentry)
                else:
                    rmtree(fullentry)
        for entry in mp3list:
            if entry not in curmp3:
                srcmp3 = cls.__inpath("mp3", entry)
                dstmp3 = os.path.join(mp3path, entry)
                copy2(srcmp3, dstmp3)

    @classmethod
    def __parallelupdatedevices(cls):
        '''__parallelupdatedevices'''
        for entry in os.listdir(cls.__paths["devices"]):
            fullentry = cls.__inpath("devices", entry)
            if entry not in [d["name"] for d in cls.__devices]:
                if os.path.isfile(fullentry):
                    os.remove(fullentry)
                else:
                    rmtree(fullentry)
        cls.__parallelize(action=cls.__updatedevice, targets=cls.__devices)

    @classmethod
    def run(cls):
        '''run'''
        cls.__preparebasepaths()
        for cls.__playlist in cls.__playlists:
            cls.__updateytplaylist()
        cls.__paralleladdsongtodb()
        cls.__parallelcopyandtag()
        cls.__parallelconvert()
        cls.__parallelupdateautoplaylist()
        cls.__parallelupdatecustomplaylist()
        cls.__parallelupdatedevices()


if __name__ == "__main__":
    Syphon().run()


#    @classmethod
#    def __test(cls, targetid):
#        '''__test'''
#        if cls.__found:
#            return
#        targetid = pickle.loads(targetid)
#        result = acoustid.lookup(cls.key, targetid[1], targetid[0])
#        if len(result['results']) is not 0:
#            cls.__found = True
#        print(result)
#
#    @classmethod
#    def __paralleltest(cls):
#        '''__paralleltest'''
#        print(cls.key)
#        con = sqlite.connect(cls.__dbfile)
#        cursor = con.cursor()
#        cursor.execute('SELECT AcoustID FROM Songs')
#        targets = [x[0] for x in cursor.fetchall()]
#        cls.__parallelize(action=cls.__test, targets=targets)
#
#    @classmethod
#    def __search(cls):
#        '''__search'''
#        con = sqlite.connect(cls.__dbfile)
#        cursor = con.cursor()
#        cursor.execute('SELECT "Input File Name", AcoustID FROM Songs')
#        targets = [{'in': x[0], 'id': x[1]} for x in cursor.fetchall()]
#        for target in targets:
#            targetid = pickle.loads(target['id'])
#            search = acoustid.lookup(cls.key, targetid[1], targetid[0])
#            if search and search['status'] == 'ok' and search['results']:
#                for result in search['results']:
#                    print(result)
#
#
#
#    @classmethod
#    def __pippo(cls):
#        '''__pippo'''
#        entries = [x for x in os.listdir(cls.__paths["devices"])]
#        for entry in entries:
#            target = cls.__inpath("devices", entry)
#            if os.path.isfile(target):
#                os.remove(target)
#            else:
#                rmtree(target)
#
