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
import csv
import logging

from datetime import datetime

COLLECTION = 'GEMINI'  # name of CAOM2 collection
observation_list = collections.OrderedDict()
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
    """
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
        yield '{}\n'.format(','.join(ii))


def get_observation(id):
    """
    Return the observation corresponding to the id
    :param id: id of the observation
    :return: observation corresponding to the id or None if such
    such observation does not exist
    """
    raise NotImplementedError('GET observation')


def _initialize_content():
    result = _read_file('/app/data/from_paul.txt')
    temp_content = {}
    for ii in result:
        result = _make_seconds(ii[1])
        formatted_result = '{},{}'.format(ii[0], ii[1])
        if result in temp_content:
            x = temp_content[result]
            x.append(formatted_result)
        else:
            temp_content[result] = [formatted_result]
    global observation_list
    observation_list = collections.OrderedDict(sorted(temp_content.items(),
                                                      key=lambda t: t[0]))


def _read_file(fqn):
    """Read the .txt file from Gemini, and make it prettier ...
    where prettier means stripping whitespace, query output text, and
    making an ISO 8601 timestamp from something that looks like this:
    ' 2018-12-17 18:19:27.334144+00 '

    or this:
    ' 2018-12-17 18:19:27+00 '

    :return a list of lists, where the inner list consists of an
        observation ID, and a last modified date/time."""
    results = []
    try:
        with open(fqn) as f:
            for row in f:
                temp = row.split('|')
                # if (not row.startswith('(') and not row.startswith('-')
                #         and len(row) > 1 and 'data_label' not in row):
                if len(temp) > 1 and 'data_label' not in row:
                    time_string = temp[3].strip().replace(' ', 'T')
                    results.append([temp[0].strip(), time_string])
    except Exception as e:
        logging.error('Could not read from csv file {}'.format(fqn))
        raise RuntimeError(e)
    return results


def _make_seconds(from_time):
    # the obs id file has the timezone information as +00, strip that for
    # returned results
    index = from_time.index('+00')
    try:
        seconds_since_epoch = datetime.strptime(from_time[:index],
                                                '%Y-%m-%dT%H:%M:%S.%f')
    except ValueError as e:
        seconds_since_epoch = datetime.strptime(from_time[:index],
                                                '%Y-%m-%dT%H:%M:%S')
    return seconds_since_epoch.timestamp()


def _subset(start_s, end_s):
    return [observation_list[x] for x in observation_list if
            (start_s <= x <= end_s)]
