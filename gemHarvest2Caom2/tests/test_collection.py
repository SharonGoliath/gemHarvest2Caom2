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
#
import os
import pytest
import sys

from mock import Mock

from caom2utils import fits2caom2
from caom2 import SimpleObservation
import caom2pipe

import gem2caom2
from gemHarvest2Caom2 import collection as c
import gem2caom2.external_metadata as em

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEST_DATA_DIR = os.path.join(THIS_DIR, 'data')
PY_VERSION = '3.6'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_invoke_gem2caom2():
    test_obs = SimpleObservation(collection='test', observation_id='1')

    def main_app2_mock():
        args = fits2caom2.get_gen_proc_arg_parser().parse_args()
        if (args is None or (args.no_validate is False) or
                (args.observation[0] != 'GEMINI') or
                (args.observation[1] != 'GS-2010A-Q-36-5-246') or
                (args.external_url[0] !=
                 'https://archive.gemini.edu/fullheader/rgS20100212S0301.fits')
                or (args.plugin != '/app/gem2caom2/gem2caom2/main_app.py') or
                (args.module != args.plugin) or
                (args.lineage !=
                 ['rgS20100212S0301/gemini:GEM/rgS20100212S0301.fits'])):
            raise RuntimeError(args)

    if em.gofr is None:
        em.gofr = gem2caom2.GemObsFileRelationship('/app/data/from_paul.txt')

    main_app_orig = gem2caom2.main_app2
    gem2caom2.main_app2 = Mock(side_effect=main_app2_mock)
    read_obs_orig = caom2pipe.manage_composable.read_obs_from_file
    caom2pipe.manage_composable.read_obs_from_file = Mock(
        return_value=test_obs)

    try:
        result = c._invoke_gem2caom2('GS-2010A-Q-36-5-246-RG')
        gem2caom2.main_app2.assert_called_with(), 'command line failure'
        assert caom2pipe.manage_composable.read_obs_from_file.called, \
            'read obs'
        assert result is not None, 'should be a mocked result'
        assert result.max_last_modified is not None, 'max last modified not set'
        assert result.last_modified is not None, 'last modified not set'
        assert result.last_modified.timestamp() == 0.0, 'wrong last modified'
    finally:
        gem2caom2.main_app2 = main_app_orig
        caom2pipe.manage_composable.read_obs_from_file = read_obs_orig


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_update_last_modified():
    test_obs = caom2pipe.manage_composable.read_obs_from_file(
        os.path.join(TEST_DATA_DIR, 'test_obs.xml'))
    assert test_obs.last_modified is None, 'wrong preconditions'
    c._update_last_modified(test_obs)
    assert test_obs.last_modified is not None, 'no result'
    assert test_obs.last_modified.timestamp() == 1539848715.277555, \
        'wrong result'
