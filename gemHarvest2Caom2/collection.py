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

from caom2pipe import manage_composable as mc

import gem2caom2
from gem2caom2 import APPLICATION, COLLECTION, SCHEME, ARCHIVE, GemName
from gem2caom2 import GemObsFileRelationship

HEADER_URL = 'https://archive.gemini.edu/fullheader/'

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
# use lazy initialization to read in the Gemini-supplied file, and
# make it's content into an in-memory searchable collection.
gofr = None


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

    global gofr
    if gofr == None:
        gofr = GemObsFileRelationship('/app/data/from_paul.txt')

    temp = gofr.subset(start, end, maxrec)
    for ii in temp:
        yield '{}\n'.format(ii)


def get_observation(obs_id):
    """
    Return the observation corresponding to the obs_id
    :param obs_id: id of the observation
    :return: observation corresponding to the id or None if such
    such observation does not exist
    """

    global gofr
    if gofr == None:
        gofr = GemObsFileRelationship('/app/data/from_paul.txt')

    if obs_id not in observation_id_list:
        logger.error(
            'Could not find file name for observation id {}'.format(obs_id))
        return None

    # query Gemini Archive for file headers based on the file id (most reliable
    # endpoint

    # file_names = observation_id_list[obs_id]
    # file_names = gofr.get_file_names(obs_id)
    # file_urls = []
    # for file_name in file_names:
    #     file_urls.append(
    #         'https://archive.gemini.edu/fullheader/{}'.format(file_name))

    obs = _invoke_gem2caom2(obs_id)
    if obs is None:
        logger.error('Could not create observation for {}'.format(obs_id))
        return None
    return obs


def _invoke_gem2caom2(obs_id):
    try:
        plugin = os.path.join(gem2caom2.__path__[0], 'main_app.py')
        logger.error(plugin)
        output_temp_file = tempfile.NamedTemporaryFile(delete=False)
        command_line_bits = _make_gem2caom2_args(obs_id)
        if len(command_line_bits) == 1:
            sys.argv = ('{} --no_validate --observation {} '
                        '--external_url {} '
                        '--plugin {} --module {} --out {} --lineage {}'.
                        format(APPLICATION, command_line_bits[0].obs_id,
                               command_line_bits[0].urls, plugin, plugin,
                               output_temp_file.name,
                               command_line_bits[0].lineage)).split()
            logger.error(sys.argv)
            gem2caom2.main_app2()
            obs = mc.read_obs_from_file(output_temp_file.name)
            os.unlink(output_temp_file.name)
            return obs
        else:
            logging.error('Wanted one observation for {}, got {}'.format(
                obs_id, command_line_bits))
            return None
    except Exception as e:
        logger.error('main_app {} failed for {} with {}'.format(
            APPLICATION, obs_id, e))
        import traceback
        logging.error(traceback.format_exc())
        return None


def _make_gem2caom2_args(obs_id):
    """Cardinality for GEMINI."""
    # DB - 07-03-19
    # TEXES Spectroscopy
    #
    # Some special code will be needed for datalabels/planes.  There are no
    # datalabels in the FITS header.  json metadata (limited) must be
    # obtained with URL like
    # https://archive.gemini.edu/jsonsummary/canonical/filepre=TX20170321_flt.2507.fits.
    # Use TX20170321_flt.2507 as datalabel.  But NOTE:  *raw.2507.fits and
    # *red.2507.fits are two planes of the same observation. I’d suggest we
    # use ‘*raw*’ as the datalabel and ‘*red*’ or ‘*raw*’ as the appropriate
    # product ID’s for the science observations.  The ‘flt’ observations do
    # not have a ‘red’ plane.  The json document contains ‘filename’ if
    # that’s helpful at all.  The ‘red’ files do not exist for all ‘raw’
    # files.

    # for each file name associated with an obs id:
    #   repair the data label (obs id)
    #   create a product id
    #
    # once the data label has been repaired, check to see if there are
    # other obs ids like the repaired one

    file_names = gofr.get_file_names(obs_id)
    logging.error('file names are {} for {}'.format(file_names, obs_id))
    # keep the obs ids and file names in sync for making
    # parameters later
    temp_params = {}
    # keep a unique list of obs ids
    repaired_obs_ids = set()
    for file_name in file_names:
        file_id = GemName.remove_extensions(file_name)
        temp = gofr.repair_data_label(file_id)
        repaired_obs_ids.add(temp)
        product_id = _make_product_id(obs_id, file_id)
        if temp in temp_params and product_id not in temp_params[temp]:
            temp_params[temp] += [product_id, file_name]
        else:
            temp_params[temp] = [product_id, file_name]

    result = []
    for ii in temp_params:  # repaired obs id
        index = 0
        temp = CommandLineBits()
        temp.obs_id = '{} {}'.format(COLLECTION, ii)

        while index < len(temp_params[ii]):
            temp.lineage += mc.get_lineage(
                ARCHIVE, temp_params[ii][index], temp_params[ii][index + 1], SCHEME)
            temp.urls += '{}{}'.format(HEADER_URL, temp_params[ii][index + 1])
            index += 2
            if index < len(temp_params[ii]):
                temp.lineage += ' '
                temp.urls += ' '
        result.append(temp)
    return result


def _make_product_id(obs_id, file_id):
    """Add the alphanumeric bits from the file id to the obs_id, to make a
    unique product id."""
    if gem2caom2.GemObsFileRelationship.is_processed(file_id):
        prefix = gem2caom2.GemObsFileRelationship._get_prefix(file_id)
        suffix = gem2caom2.GemObsFileRelationship._get_suffix(file_id)
        if len(prefix) > 0:
            removals = [prefix] + suffix
        else:
            removals = suffix
        # take prefixes and suffixes off the obs id, in whatever form it might
        # exist
        repaired = obs_id
        for ii in removals:
            repaired = repaired.split(ii, 1)[0]
            repaired = repaired.split(ii.upper(), 1)[0]
            repaired = repaired.rstrip('-')
            repaired = repaired.rstrip('_')
        # put prefixes and suffixes back, all as suffixes, in upper case, with
        # dashes
        result = '-'.join(ii.upper() for ii in removals)
        logging.error('result is {}'.format(result))
        result = repaired if len(result) == 0 else repaired + '-' + result
    else:
        result = obs_id
        logging.error('unprocessed {}'.format(file_id))
    return result


class CommandLineBits(object):
    """Convenience class to keep the bits of command-line that are
    inter-connected together."""

    def __init__(self, obs_id='', lineage='', urls=''):
        self.obs_id = obs_id
        self.lineage = lineage
        self.urls = urls

    @property
    def obs_id(self):
        return self._obs_id

    @obs_id.setter
    def obs_id(self, value):
        self._obs_id = value

    @property
    def lineage(self):
        return self._lineage

    @lineage.setter
    def lineage(self, value):
        self._lineage = value

    @property
    def urls(self):
        return self._urls

    @urls.setter
    def urls(self, value):
        self._urls = value
