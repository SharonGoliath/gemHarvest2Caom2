# -*- coding: utf-8 -*-
# ***********************************************************************
# ******************  CANADIAN ASTRONOMY DATA CENTRE  *******************
# *************  CENTRE CANADIEN DE DONNÉES ASTRONOMIQUES  **************
#
#  (c) 2019.                            (c) 2019.
#  Government of Canada                 Gouvernement du Canada
#  National Research Council            Conseil national de recherches
#  Ottawa, Canada, K1A 0R6              Ottawa, Canada, K1A 0R6
#  All rights reserved                  Tous droits réservés
#
#  NRC disclaims any warranties,        Le CNRC dénie toute garantie
#  expressed, implied, or               énoncée, implicite ou légale,
#  statutory, of any kind with          de quelque nature que ce
#  respect to the software,             soit, concernant le logiciel,
#  including without limitation         y compris sans restriction
#  any warranty of merchantability      toute garantie de valeur
#  or fitness for a particular          marchande ou de pertinence
#  purpose. NRC shall not be            pour un usage particulier.
#  liable in any event for any          Le CNRC ne pourra en aucun cas
#  damages, whether direct or           être tenu responsable de tout
#  indirect, special or general,        dommage, direct ou indirect,
#  consequential or incidental,         particulier ou général,
#  arising from the use of the          accessoire ou fortuit, résultant
#  software.  Neither the name          de l'utilisation du logiciel. Ni
#  of the National Research             le nom du Conseil National de
#  Council of Canada nor the            Recherches du Canada ni les noms
#  names of its contributors may        de ses  participants ne peuvent
#  be used to endorse or promote        être utilisés pour approuver ou
#  products derived from this           promouvoir les produits dérivés
#  software without specific prior      de ce logiciel sans autorisation
#  written permission.                  préalable et particulière
#                                       par écrit.
#
#  This file is part of the             Ce fichier fait partie du projet
#  OpenCADC project.                    OpenCADC.
#
#  OpenCADC is free software:           OpenCADC est un logiciel libre ;
#  you can redistribute it and/or       vous pouvez le redistribuer ou le
#  modify it under the terms of         modifier suivant les termes de
#  the GNU Affero General Public        la “GNU Affero General Public
#  License as published by the          License” telle que publiée
#  Free Software Foundation,            par la Free Software Foundation
#  either version 3 of the              : soit la version 3 de cette
#  License, or (at your option)         licence, soit (à votre gré)
#  any later version.                   toute version ultérieure.
#
#  OpenCADC is distributed in the       OpenCADC est distribué
#  hope that it will be useful,         dans l’espoir qu’il vous
#  but WITHOUT ANY WARRANTY;            sera utile, mais SANS AUCUNE
#  without even the implied             GARANTIE : sans même la garantie
#  warranty of MERCHANTABILITY          implicite de COMMERCIALISABILITÉ
#  or FITNESS FOR A PARTICULAR          ni d’ADÉQUATION À UN OBJECTIF
#  PURPOSE.  See the GNU Affero         PARTICULIER. Consultez la Licence
#  General Public License for           Générale Publique GNU Affero
#  more details.                        pour plus de détails.
#
#  You should have received             Vous devriez avoir reçu une
#  a copy of the GNU Affero             copie de la Licence Générale
#  General Public License along         Publique GNU Affero avec
#  with OpenCADC.  If not, see          OpenCADC ; si ce n’est
#  <http://www.gnu.org/licenses/>.      pas le cas, consultez :
#                                       <http://www.gnu.org/licenses/>.
#
#  $Revision: 4 $
#
# ***********************************************************************
#

import collections
import logging
import os
import sys
import tempfile

from datetime import datetime
from datetime import timezone

from caom2pipe import manage_composable as mc

import gem2caom2
from gem2caom2 import APPLICATION, COLLECTION, SCHEME, ARCHIVE, GemName
from gem2caom2 import main_app as gem_main_app

# COLLECTION = 'GEMINI'  # name of CAOM2 collection
# SCHEME = 'gemini'
# ARCHIVE = 'GEM'

# in-memory structure that contains the obs-id, last modified information
# by timestamp, making it possible to identify time-bounded lists of
# observations
observation_list = collections.OrderedDict()
# in-memory structure that contains the file name to obs id relationship,
# making it possible to execute queries against the Gemini Science Archive
# by observation ID
observation_id_list = {}
logger = logging.getLogger('caom2proxy')
logger.setLevel(logging.DEBUG)


def list_observations(start=None, end=None, maxrec=None):
    """
    List observations
    :param start: start date (UTC)
    :param end: end date (UTC)
    :param maxrec: maximum number of rows to return
    :return: Comma separated list, each row consisting of ObservationID,
    last mod date.

    NOTE: For stream the results use a generator, e.g:
        for i in range(3):
        yield "{}\n".format(datetime.datetime.now().isoformat())
        time.sleep(1)

    The results of this query come from a file supplied by Gemini
    approximately once every six months. That file is located on this
    container. It's read in and re-formatted into the information required
    to support this endpoint query.
    """

    # use lazy initialization to read in the Gemini-supplied file, and
    # make it's content into an in-memory searchable collection.
    if len(observation_list) < 1:
        _initialize_content()

    if start is not None and end is not None:
        temp = _subset(start.timestamp(), end.timestamp())
    elif start is not None:
        temp = _subset(start.timestamp(), datetime.now().timestamp())
    elif end is not None:
        temp = _subset(0, end.timestamp())
    else:
        temp = _subset(0, datetime.now().timestamp())
    if maxrec is not None:
        temp = temp[:maxrec]
    for ii in temp:
        yield '{}\n'.format(ii)


def get_observation(obs_id):
    """
    Return the observation corresponding to the obs_id
    :param obs_id: id of the observation
    :return: observation corresponding to the id or None if such
    such observation does not exist
    """

    # use lazy initialization to read in the Gemini-supplied file, and
    # make its content into an in-memory searchable collection.
    if len(observation_list) < 1:
        _initialize_content()

    if obs_id not in observation_id_list:
        logger.error(
            'Could not find file name for observation id {}'.format(obs_id))
        return None

    # query Gemini Archive for file headers based on the file id (most reliable
    # endpoint

    file_names = observation_id_list[obs_id]
    file_urls = []
    for file_name in file_names:
        file_urls.append(
            'https://archive.gemini.edu/fullheader/{}'.format(file_name))

    obs = _invoke_gem2caom2(file_urls, obs_id, file_names)
    if obs is None:
        logger.error('Could not create observation for {}'.format(obs_id))
        return None
    return obs


def _invoke_gem2caom2(external_urls, obs_id, input_file_names):
    try:
        plugin = os.path.join(gem2caom2.__path__[0], 'main_app.py')
        logger.error(plugin)
        output_temp_file = tempfile.NamedTemporaryFile(delete=False)
        if len(input_file_names) > 1:
            temp = []
            for file_name in input_file_names:
                file_id = GemName.remove_extensions(file_name)
                if file_id.endswith('m'):
                    product_id = 'intensity'
                elif file_id.endswith('i'):
                    product_id = 'norm_intensity'
                else:
                    product_id = obs_id
                temp.append(
                    mc.get_lineage(ARCHIVE, product_id, file_name, SCHEME))
            lineage = ' '.join(i for i in temp)
        else:
            lineage = mc.get_lineage(ARCHIVE, obs_id, input_file_names[0],
                                     SCHEME)
        external_url = ' '.join(i for i in external_urls)

        sys.argv = ('{} --no_validate --observation {} {} '
                    '--external_url {} '
                    '--plugin {} --module {} --out {} --lineage {}'.
                    format(APPLICATION, COLLECTION, obs_id, external_url,
                           plugin, plugin, output_temp_file.name,
                           lineage)).split()
        logger.error(sys.argv)
        gem2caom2.main_app2()
        obs = mc.read_obs_from_file(output_temp_file.name)
        os.unlink(output_temp_file.name)
        return obs
    except Exception as e:
        logger.error('main_app {} failed for {} with {}'.format(
            APPLICATION, obs_id, e))
        import traceback
        logging.error(traceback.format_exc())
        return None


def _initialize_content():
    """Initialize the internal data structures that represents the
    query list from the Gemini Science Archive.

    observation_list structure: a dict, keys are last modified time,
        values are a set of observation IDs with that last modified time

    observation_id_list structure: a dict, keys are observation ID,
        values are a set of associated file names
    """
    result = _read_file('/app/data/from_paul.txt')
    # result row structure:
    # 0 = data label
    # 1 = timestamp
    # 2 = file name
    global observation_id_list
    temp_content = {}
    for ii in result:
        # re-organize to be able to answer list_observations queries
        ol_key = _make_seconds(ii[1])
        if ol_key in temp_content:
            if ii[0] not in temp_content[ol_key]:
                temp_content[ol_key].append(ii[0])
        else:
            temp_content[ol_key] = [ii[0]]
        # re-organize to be able to answer get_observation queries
        if ii[0] in observation_id_list:
            observation_id_list[ii[0]].append(ii[2])
        else:
            observation_id_list[ii[0]] = [ii[2]]

    # this structure means an observation ID occurs more than once with
    # different last modified times
    global observation_list
    observation_list = collections.OrderedDict(sorted(temp_content.items(),
                                                      key=lambda t: t[0]))
    logger.debug('Observation list initialized in memory.')


def _read_file(fqn):
    """Read the .txt file from Gemini, and make it prettier ...
    where prettier means stripping whitespace, query output text, and
    making an ISO 8601 timestamp from something that looks like this:
    ' 2018-12-17 18:19:27.334144+00 '

    or this:
    ' 2018-12-17 18:19:27+00 '

    :return a list of lists, where the inner list consists of an
        observation ID, a last modified date/time, and a file name.

    File structure indexes:
    0 == data label
    1 == file name
    3 == last modified date/time
    """
    results = []
    try:
        with open(fqn) as f:
            for row in f:
                temp = row.split('|')
                if len(temp) > 1 and 'data_label' not in row:
                    time_string = temp[3].strip().replace(' ', 'T')
                    if len(temp[0].strip()) > 1:
                        results.append(
                            [temp[0].strip(), time_string, temp[1].strip()])
                    else:
                        # no data label in the file, so use the file name
                        results.append(
                            [temp[1].strip(), time_string, temp[1].strip()])

    except Exception as e:
        logger.error('Could not read from csv file {}'.format(fqn))
        raise RuntimeError(e)
    return results


def _make_seconds(from_time):
    """Deal with the different time formats in the Gemini-supplied file
    to get the number of seconds since the epoch, to serve as an
    ordering index for the list of observation IDs.

    The obs id file has the timezone information as +00, strip that for
    returned results.
    """
    index = from_time.index('+00')
    try:
        seconds_since_epoch = datetime.strptime(from_time[:index],
                                                '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError as e:
        seconds_since_epoch = datetime.strptime(from_time[:index],
                                                '%Y-%m-%dT%H:%M:%S')
    return seconds_since_epoch.timestamp()


def _subset(start_s, end_s):
    """Get only part of the observation list, limited by timestamps."""
    logger.debug('Timestamp endpoints are between {} and {}.'.format(
        start_s, end_s))
    global observation_list
    temp = []
    for ii in observation_list:
        if start_s <= ii <= end_s:
            for jj in observation_list[ii]:
                temp.append(
                    '{},{}'.format(jj, datetime.fromtimestamp(ii, timezone.utc).isoformat()))
        if ii > end_s:
            break
    return temp
