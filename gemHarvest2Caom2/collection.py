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

import logging
import os
import sys
import tempfile

from datetime import datetime
from datetime import timedelta

from caom2pipe import manage_composable as mc
from caom2pipe import execute_composable as ex_comp

import gem2caom2
import gem2caom2.external_metadata as em
from gem2caom2 import APPLICATION, SCHEME, ARCHIVE, GemName
from gem2caom2 import GemObsFileRelationship, CommandLineBits

logger = logging.getLogger('caom2proxy')
logger.setLevel(logging.DEBUG)

COLLECTION = 'GEMINI'


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

    if em.gofr is None:
        em.gofr = GemObsFileRelationship('/app/data/from_paul.txt')

    temp = em.gofr.subset(start, end, maxrec)
    for ii in temp:
        yield '{}\n'.format(ii)


def get_observation(obs_id):
    """
    Return the observation corresponding to the obs_id
    :param obs_id: id of the observation
    :return: observation corresponding to the id or None if such
    such observation does not exist
    """

    if em.gofr is None:
        em.gofr = GemObsFileRelationship('/app/data/from_paul.txt')

    obs = _invoke_gem2caom2(obs_id)
    if obs is None:
        logger.error('Could not create observation for {}'.format(obs_id))
        return None
    return obs


def _invoke_gem2caom2(obs_id):
    try:
        plugin = os.path.join(gem2caom2.__path__[0], 'main_app.py')
        output_temp_file = tempfile.NamedTemporaryFile(delete=False)
        command_line_bits = em.gofr.get_args(obs_id)
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
            _update_last_modified(obs)
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


def _update_last_modified(obs):
    max_last_modified = timedelta()
    for p in obs.planes:
        for a in obs.planes[p].artifacts:
            file_name = ex_comp.CaomName(
                obs.planes[p].artifacts[a].uri).file_name
            ts = em.gofr.get_timestamp(file_name)
            max_last_modified = max(ts, max_last_modified)
            ts_dt = datetime.fromtimestamp(ts.total_seconds())
            obs.planes[p].max_last_modified = ts_dt
            obs.planes[p].artifacts[a].max_last_modified = ts_dt
            obs.planes[p].last_modified = ts_dt
            obs.planes[p].artifacts[a].last_modified = ts_dt
            for pt in obs.planes[p].artifacts[a].parts:
                obs.planes[p].artifacts[a].parts[pt].max_last_modified = ts_dt
                obs.planes[p].artifacts[a].parts[pt].last_modified = ts_dt
                for c in obs.planes[p].artifacts[a].parts[pt].chunks:
                    c.max_last_modified = ts_dt
                    c.last_modified = ts_dt
    max_dt = datetime.fromtimestamp(max_last_modified.total_seconds())
    obs.max_last_modified = max_dt
    obs.last_modified = max_dt
