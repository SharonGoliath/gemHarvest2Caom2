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

import pytest
import sys

from datetime import datetime
from mock import Mock

from caom2 import SimpleObservation, DataProductType, CalibrationLevel
from caom2utils import fits2caom2
import caom2pipe

import gem2caom2
from gemHarvest2Caom2 import collection as c

ISO_DATE = '%Y-%m-%dT%H:%M:%S.%f'
PY_VERSION = '3.6'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_list_observations_all():
    temp = c.list_observations()
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-CAL20170616-11-022,2017-06-19T03:21:29.345417'), \
        'wrong content'
    assert len(list(temp)) == 171, 'wrong count'
    assert 'GN-2015B-Q-1-12-1003' in c.observation_id_list, 'init failed'
    assert c.observation_id_list['GN-2015B-Q-1-12-1003'] == \
        ['N20150807G0044m.fits', 'N20150807G0044i.fits', 'N20150807G0044.fits'], \
        'entry missing'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_list_observations_only_start():
    start = datetime.strptime('2018-12-16T03:47:03.939488', ISO_DATE)
    temp = c.list_observations(start=start)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-2018B-FT-113-24-015,2018-12-17T18:08:29.362826+00'), \
        'wrong content'
    assert len(list(temp)) == 97, 'wrong count'

    temp = c.list_observations(start=start, maxrec=3)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-2018B-FT-113-24-015,2018-12-17T18:08:29.362826+00'), \
        'wrong content'
    assert len(list(temp)) == 2, 'wrong maxrec count'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_list_observations_only_end():
    end = datetime.strptime('2018-12-16T18:12:26.16614', ISO_DATE)
    temp = c.list_observations(end=end)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-CAL20170616-11-022,2017-06-19T03:21:29.345417+00'), \
        'wrong content'
    assert len(list(temp)) == 73, 'wrong count'

    temp = c.list_observations(end=end, maxrec=3)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-CAL20170616-11-022,2017-06-19T03:21:29.345417+00'), \
        'wrong content'
    assert len(list(temp)) == 2, 'wrong maxrec count'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_list_observations_start_end():
    start = datetime.strptime('2017-06-20T12:36:35.681662', ISO_DATE)
    end = datetime.strptime('2017-12-17T20:13:56.572387', ISO_DATE)
    temp = c.list_observations(start=start, end=end)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GS-CAL20150505-4-035,2017-06-20T21:56:27.861470+00'), \
        'wrong content'
    assert len(list(temp)) == 62, 'wrong count'

    temp = c.list_observations(start=start, end=end, maxrec=3)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GS-CAL20150505-4-035,2017-06-20T21:56:27.861470+00'), \
        'wrong content'
    assert len(list(temp)) == 2, 'wrong maxrec count'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_invoke_gem2caom2():
    def read_obs_from_file_mock(fqn):
        return SimpleObservation(collection='test', observation_id='1')

    def main_app2_mock():
        args = fits2caom2.get_gen_proc_arg_parser().parse_args()
        if (args is None or (args.no_validate is False) or
                (args.observation[0] != 'GEMINI') or
                (args.observation[1] != 'test_obs_id') or
                (args.external_url[0] != 'test_url')  or
                (args.plugin != '/app/gem2caom2/gem2caom2/main_app.py') or
                (args.module != args.plugin) or
                (args.lineage != ['test_obs_id/gemini:GEM/test_input_file_name'])):
            raise RuntimeError(args)

    main_app_orig = gem2caom2.main_app2
    gem2caom2.main_app2 = Mock(side_effect=main_app2_mock)
    read_obs_orig = caom2pipe.manage_composable.read_obs_from_file
    caom2pipe.manage_composable.read_obs_from_file = Mock(
        return_value=read_obs_from_file_mock)

    try:
        result = c._invoke_gem2caom2(['test_url'], 'test_obs_id',
                                     ['test_input_file_name'])
        gem2caom2.main_app2.assert_called_with(), 'command line failure'
        assert caom2pipe.manage_composable.read_obs_from_file.called, \
            'read obs'
        assert result is not None, 'should be a mocked result'
    finally:
        gem2caom2.main_app2 = main_app_orig
        caom2pipe.manage_composable.read_obs_from_file = read_obs_orig
