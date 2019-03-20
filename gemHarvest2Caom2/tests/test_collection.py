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
import pytest
import sys

from mock import Mock

from caom2utils import fits2caom2
from caom2 import SimpleObservation
import caom2pipe

import gem2caom2
from gemHarvest2Caom2 import collection as c

PY_VERSION = '3.6'


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_invoke_gem2caom2():
    def read_obs_from_file_mock(fqn):
        return SimpleObservation(collection='test', observation_id='1')

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
                 ['GS-2010A-Q-36-5-246-RG/gemini:GEM/rgS20100212S0301.fits'])):
            raise RuntimeError(args)

    if c.gofr is None:
        c.gofr = gem2caom2.GemObsFileRelationship('/app/data/from_paul.txt')

    main_app_orig = gem2caom2.main_app2
    gem2caom2.main_app2 = Mock(side_effect=main_app2_mock)
    read_obs_orig = caom2pipe.manage_composable.read_obs_from_file
    caom2pipe.manage_composable.read_obs_from_file = Mock(
        return_value=read_obs_from_file_mock)

    try:
        result = c._invoke_gem2caom2('GS-2010A-Q-36-5-246-RG')
        gem2caom2.main_app2.assert_called_with(), 'command line failure'
        assert caom2pipe.manage_composable.read_obs_from_file.called, \
            'read obs'
        assert result is not None, 'should be a mocked result'
    finally:
        gem2caom2.main_app2 = main_app_orig
        caom2pipe.manage_composable.read_obs_from_file = read_obs_orig


# TEST_SUBJECTS = {
#     'GS20141226S0203_BIAS': ['GS-CAL20141226-7-026-G-BIAS', em.Inst.GMOS,
#                              'GS-CAL20141226'],
#     'N20070819S0339_dark': ['GN-2007B-Q-107-150-004-DARK', em.Inst.GMOS,
#                             'GN-2007B-Q-107'],
#     'N20110927S0170_fringe': ['GN-CAL20110927-900-170-FRINGE', em.Inst.GMOS,
#                               'GN-CAL20110927'],
#     'N20120320S0328_stack_fringe': ['GN-CAL20120320-900-328-STACK-FRINGE',
#                                     em.Inst.GMOS, 'GN-CAL20120320'],
#     'N20130404S0512_flat': ['GN-2013A-Q-63-54-051-FLAT', em.Inst.GMOS,
#                             'GN-2013A-Q-63'],
#     'N20140313S0072_flat': ['GN-2013B-Q-75-163-011-FLAT', em.Inst.GMOS,
#                             'GN-2013B-Q-75'],
#     'N20141109S0266_bias': ['GN-CAL20141109-2-001-BIAS', em.Inst.GMOS,
#                             'GN-CAL20141109'],
#     'N20150804S0348_dark': ['GN-2015B-Q-53-138-061-DARK', em.Inst.GMOS,
#                             'GN-2015B-Q-53'],
#     'N20160403S0236_flat_pasted': ['GN-CAL20160404-7-017-FLAT-PASTED',
#                                    em.Inst.GMOS, 'GN-CAL20160404'],
#     'S20120922S0406': ['GS-2012B-Q-1-32-002', em.Inst.GMOS, 'GS-2012B-Q-1'],
#     'S20131007S0067_fringe': ['GS-CAL20131007-900-067-FRINGE', em.Inst.GMOS,
#                               'GS-CAL20131007'],
#     'S20140124S0039_dark': ['GS-2013B-Q-16-277-019-DARK', em.Inst.GMOS,
#                             'GS-2013B-Q-16'],
#     'S20141129S0331_dark': ['GS-CAL20141129-1-001-DARK', em.Inst.GMOS,
#                             'GS-CAL20141129'],
#     'S20161227S0051': ['GS-CAL20161227-5-001', em.Inst.GMOS, 'GS-CAL20161227'],
#     'fmrgN20020413S0120_add': ['GN-2002A-SV-78-7-003-FMRG-ADD', em.Inst.GMOS,
#                                'GN-2002A-SV-78'],
#     'gS20181219S0216_flat': ['GS-CAL20181219-4-021-G-FLAT', em.Inst.GMOS,
#                              'GS-CAL20181219'],
#     'gS20190301S0556_bias': ['GS-CAL20190301-4-046-G-BIAS', em.Inst.GMOS,
#                              'GS-CAL20190301'],
#     'mfrgS20041117S0073_add': ['GS-2004B-Q-42-1-001-MFRG-ADD', em.Inst.GMOS,
#                                'GS-2004B-Q-42'],
#     'mfrgS20160310S0154_add': ['GS-2016A-Q-7-175-001-MFRG-ADD', em.Inst.GMOS,
#                                'GS-2016A-Q-7'],
#     'mrgN20041016S0095': ['GN-2004B-Q-30-1-001-MRG', em.Inst.GMOS,
#                           'GN-2004B-Q-30'],
#     'mrgN20050831S0770_add': ['GN-2005B-Q-28-32-001-MRG-ADD', em.Inst.GMOS,
#                               'GN-2005B-Q-28'],
#     'mrgN20160311S0691_add': ['GN-2016A-Q-68-46-001-MRG-ADD', em.Inst.GMOS,
#                               'GN-2016A-Q-68'],
#     'mrgS20120922S0406': ['GS-2012B-Q-1-32-002-MRG', em.Inst.GMOS,
#                           'GS-2012B-Q-1'],
#     'mrgS20160901S0122_add': ['GS-2016B-Q-72-23-001-MRG-ADD', em.Inst.GMOS,
#                               'GS-2016B-Q-72'],
#     'mrgS20181016S0184_fringe': ['GS-CAL20181016-5-001-MRG-FRINGE',
#                                  em.Inst.GMOS, 'GS-CAL20181016'],
#     'rS20121030S0136': ['GS-2012B-Q-90-366-003-R', em.Inst.GMOS,
#                         'GS-2012B-Q-90'],
#     'rgS20100212S0301': ['GS-2010A-Q-36-5-246', em.Inst.GMOS,
#                          'GS-2010A-Q-36'],
#     'rgS20100316S0366': ['GS-2010A-Q-36-6-358', em.Inst.GMOS, 'GS-2010A-Q-36'],
#     'rgS20130103S0098_FRINGE': ['GS-CAL20130103-3-001-RG-FRINGE', em.Inst.GMOS,
#                                 'GS-CAL20130103'],
#     'rgS20131109S0166_FRINGE': ['GS-CAL20131109-17-001-RG-FRINGE', em.Inst.GMOS,
#                                 'GS-CAL20131109'],
#     'rgS20161227S0051_fringe': ['GS-CAL20161227-5-001-RG-FRINGE', em.Inst.GMOS,
#                                 'GS-CAL20161227'],
#     'p2004may20_0048_FLAT': ['GS-CAL20040520-7-0048-P-FLAT', em.Inst.PHOENIX,
#                              'GS-CAL20040520'],
#     'p2004may19_0255_COMB': ['GS-2004A-Q-6-27-0255-P-COMB', em.Inst.PHOENIX,
#                              'GS-2004A-Q-6'],
#     'P2003JAN14_0148_DARK': ['GS-CAL20030114-7-0148', em.Inst.PHOENIX,
#                              'GS-CAL2003011'],
#     'P2002FEB03_0045_DARK10SEC': ['GS-CAL20020203-4-0045', em.Inst.PHOENIX,
#                                   'GS-CAL20020203'],
#     'P2002DEC02_0161_SUB': ['GS-2002B-Q-22-13-0161', em.Inst.PHOENIX,
#                             'GS-2002B-Q-22'],
#     'P2002DEC02_0161_SUB.0001': ['GS-2002B-Q-22-13-0161', em.Inst.PHOENIX,
#                                  'GS-2002B-Q-22'],
#     'P2002DEC02_0075_SUB.0001': ['GS-CAL20021202-3-0075', em.Inst.PHOENIX,
#                                  'GS-CAL2002120'],
#     '2004may19_0255': ['GS-2004A-Q-6-27-0255', em.Inst.PHOENIX, 'GS-2004A-Q-6'],
#     'S20181016S0184': ['GS-CAL20181016-5-001', em.Inst.GMOS,
#                        'GS-CAL20181016-5'],
#     '2002dec02_0161': ['GS-2002B-Q-22-13-0161', em.Inst.PHOENIX,
#                        'GS-2002B-Q-22']
#
# }

y = 'https://archive.gemini.edu/fullheader/'
z = 'gemini:GEM/'
x = {
    'GS-2002B-Q-22-13-0161': [c.CommandLineBits(
        obs_id='GEMINI GS-2002B-Q-22-13-0161',
        urls='{}{} {}{} {}{}'.format(
            y, 'P2002DEC02_0161_SUB.0001.fits', y,
            'P2002DEC02_0161_SUB.fits', y,
            '2002dec02_0161.fits'),
        lineage='{}/{}{} {}/{}{} {}/{}{}'.format(
            'GS-2002B-Q-22-13-0161-SUB-0001', z,
            'P2002DEC02_0161_SUB.0001.fits',
            'GS-2002B-Q-22-13-0161-SUB', z,
            'P2002DEC02_0161_SUB.fits',
            'GS-2002B-Q-22-13-0161', z,
            '2002dec02_0161.fits'))
    ],
    'GN-2015B-Q-1-12-1003': [c.CommandLineBits(
        obs_id='GEMINI GN-2015B-Q-1-12-1003',
        urls='{}{} {}{} {}{}'.format(
            y, 'N20150807G0044m.fits', y,
            'N20150807G0044i.fits', y,
            'N20150807G0044.fits', ),
        lineage='{}/{}{} {}/{}{} {}/{}{}'.format(
            'GN-2015B-Q-1-12-1003-M', z,
            'N20150807G0044m.fits',
            'GN-2015B-Q-1-12-1003-I', z,
            'N20150807G0044i.fits',
            'GN-2015B-Q-1-12-1003', z,
            'N20150807G0044.fits'))
    ],
    'GS-2010A-Q-36-5-246-RG': [c.CommandLineBits(
        obs_id='GEMINI GS-2010A-Q-36-5-246',
        urls='{}{}'.format(y, 'rgS20100212S0301.fits'),
        lineage='{}/{}{}'.format(
            'GS-2010A-Q-36-5-246-RG', z, 'rgS20100212S0301.fits'))
    ]
}


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_make_gem2caom2_args():
    if c.gofr is None:
        c.gofr = gem2caom2.GemObsFileRelationship('/app/data/from_paul.txt')

    for ii in x:
        test_result = c._make_gem2caom2_args(ii)
        assert test_result is not None, 'no result'
        assert len(test_result) == len(x[ii]), 'wrong number of results'
        index = 0
        for jj in test_result:
            assert jj.obs_id == x[ii][index].obs_id, \
                'obs id {} {}'.format(ii, jj)
            assert jj.lineage == x[ii][index].lineage, \
                'lineage {} {}'.format(ii, jj)
            assert jj.urls == x[ii][index].urls, \
                'urls {} {}'.format(ii, jj)
            index += 1


@pytest.mark.skipif(not sys.version.startswith(PY_VERSION),
                    reason='support 3.6 only')
def test_make_product_id():
    test_file = 'P2002DEC02_0161_SUB'
    test_prefix = gem2caom2.GemObsFileRelationship._get_prefix(test_file)
    assert test_prefix == '', 'prefix wrong {}'.format(test_prefix)
    test_suffix = gem2caom2.GemObsFileRelationship._get_suffix(test_file)
    assert test_suffix == ['SUB'], 'suffix wrong {}'.format(test_suffix)
    test_result = c._make_product_id('GS-2002B-Q-22-13-0161', test_file)
    assert test_result == 'GS-2002B-Q-22-13-0161-SUB', \
        'product id wrong {}'.format(test_result)

    test_file = 'P2002DEC02_0161_SUB.0001'
    test_prefix = gem2caom2.GemObsFileRelationship._get_prefix(test_file)
    assert test_prefix == '', 'prefix wrong {}'.format(test_prefix)
    test_suffix = gem2caom2.GemObsFileRelationship._get_suffix(test_file)
    assert test_suffix == ['SUB-0001'], 'suffix wrong {}'.format(test_suffix)
    test_result = c._make_product_id('GS-2002B-Q-22-13-0161', test_file)
    assert test_result == 'GS-2002B-Q-22-13-0161-SUB-0001', \
        'product id wrong {}'.format(test_result)

    test_file = 'N20150807G0044i'
    test_prefix = gem2caom2.GemObsFileRelationship._get_prefix(test_file)
    assert test_prefix == '', 'prefix wrong {}'.format(test_prefix)
    test_suffix = gem2caom2.GemObsFileRelationship._get_suffix(test_file)
    assert test_suffix == ['i'], 'suffix wrong {}'.format(test_suffix)
    test_result = c._make_product_id('GN-2015B-Q-1-12-1003', test_file)
    assert test_result == 'GN-2015B-Q-1-12-1003-I', \
        'product id wrong {}'.format(test_result)

    # test the 'do not change' the data label option
    test_file = 'N20150807G0044'
    test_prefix = gem2caom2.GemObsFileRelationship._get_prefix(test_file)
    assert test_prefix == '', 'prefix wrong {}'.format(test_prefix)
    test_suffix = gem2caom2.GemObsFileRelationship._get_suffix(test_file)
    assert test_suffix == [], 'suffix wrong {}'.format(test_suffix)
    test_result = c._make_product_id('GN-2015B-Q-1-12-1003', test_file)
    assert test_result == 'GN-2015B-Q-1-12-1003', \
        'product id wrong {}'.format(test_result)

    test_file = '2002dec02_0161'
    test_result = c._make_product_id('GS-2002B-Q-22-13-0161', test_file)
    assert test_result == 'GS-2002B-Q-22-13-0161', \
        'product id wrong {}'.format(test_result)
