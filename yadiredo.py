#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import shutil
import pathlib
import os
import argparse
import hashlib
import requests
import logging as log
from pprint import pformat
from time import sleep


class Yadiredo:
    API_ENDPOINT = 'https://cloud-api.yandex.net/v1/disk/public/resources/?public_key={}&path=/{}&offset={}'

    def __init__(self, verify_only, verify_checksums, delay, *args, **kwargs):
        self.verify_only = verify_only
        self.verify_checksums = verify_checksums
        self.delay = delay

    @classmethod
    def _md5sum(cls, file_path):
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            for chunk in iter(lambda: f.read(128 * md5.block_size), b''):
                md5.update(chunk)
        return md5.hexdigest()

    @classmethod
    def _download_file(cls, target_path, url):
        log.info('downloading ...')
        r = requests.get(url, stream=True)
        with open(target_path, 'wb') as f:
            shutil.copyfileobj(r.raw, f)

    def _check_local_file(self, target_path, size, checksum):
        if os.path.isfile(target_path):
            if size == os.path.getsize(target_path):
                if not self.verify_checksums or checksum == self._md5sum(target_path):
                    return True
                else:
                    log.warning('checksum mismatch')
            else:
                log.warning('size mismatch')
        else:
            log.debug('missing in target dir')
        return False

    def _try_as_file(self, j, current_path, source_path):
        if 'file' in j:
            file_save_path = os.path.join(current_path, j['name'])
            log.info(f'processing "{source_path}/{j["name"]}"')
            if not self._check_local_file(file_save_path, j['size'], j['md5']):
                if not self.verify_only:
                    self._download_file(file_save_path, j['file'])
            else:
                log.info('already downloaded, checksums correct')
            return True
        return False

    def download_path(self, target_path, public_key, source_path, offset=0):
        sleep(self.delay)
        log.info('getting "/{}"'.format(source_path))
        log.debug('offset {}'.format(offset))
        current_path = os.path.join(target_path, source_path)
        pathlib.Path(current_path).mkdir(parents=True, exist_ok=True)
        jsn = requests.get(self.API_ENDPOINT.format(public_key, source_path, offset)).json()

        # first try to treat the actual json as a single file description
        if self._try_as_file(jsn, current_path, source_path):
            return

        # otherwise treat it as a directory
        try:
            emb = jsn['_embedded']
        except KeyError:
            log.error('object should be a directory, but it is not:\n' + pformat(jsn))
            return
        items = emb['items']
        for i in items:
            # each item can be a file...
            if self._try_as_file(i, current_path, source_path):
                continue
            # ... or a directory
            else:
                subdir_path = os.path.join(source_path, i['name'])
                self.download_path(target_path, public_key, subdir_path)

        # check if current directory has more items
        last = offset + emb['limit']
        if last < emb['total']:
            self.download_path(target_path, public_key, source_path, last)


def main():
    log.basicConfig(level=log.INFO)

    parser = argparse.ArgumentParser(description='Yandex.Disk downloader.')
    parser.add_argument('url')
    parser.add_argument('-o', dest='output_path', default='output')
    parser.add_argument('--verify_only', action='store_const', const=True, default=False)
    parser.add_argument('--verify_checksums', action='store_const', const=True, default=False)
    parser.add_argument('--delay', type=int, action='store', default=0.1)
    #parser.add_argument('-r', dest='retries', default=None)
    args = parser.parse_args()
    #TODO: HTTPAdapter + urllib Retry
    #if args.retries:
    #    requests.adapters.DEFAULT_RETRIES = args.retries

    d = Yadiredo(**vars(args))
    d.download_path(args.output_path, args.url, '')


if __name__ == '__main__':
    main()
