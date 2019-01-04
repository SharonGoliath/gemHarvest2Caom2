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

import json
import pytest
import sys

from datetime import datetime

from gemHarvest2Caom2 import collection as c
ISO_DATE = '%Y-%m-%dT%H:%M:%S.%f'


@pytest.mark.skipif(not sys.version.startswith('3.6'),
                    reason='support 3.6 only')
def test_list_observations_all():
    temp = c.list_observations()
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-CAL20170616-11-022,2017-06-19T03:21:29.345417+00'), \
        'wrong content'
    assert len(list(temp)) == 158, 'wrong count'


@pytest.mark.skipif(not sys.version.startswith('3.6'),
                    reason='support 3.6 only')
def test_list_observations_only_start():
    start = datetime.strptime('2018-12-16T03:47:03.939488', ISO_DATE)
    temp = c.list_observations(start=start)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-2018B-FT-113-24-015,2018-12-17T18:08:29.362826+00'), \
        'wrong content'
    assert len(list(temp)) == 89, 'wrong count'

    temp = c.list_observations(start=start, maxrec=3)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-2018B-FT-113-24-015,2018-12-17T18:08:29.362826+00'), \
        'wrong content'
    assert len(list(temp)) == 2, 'wrong maxrec count'


@pytest.mark.skipif(not sys.version.startswith('3.6'),
                    reason='support 3.6 only')
def test_list_observations_only_end():
    end = datetime.strptime('2018-12-16T18:12:26.16614', ISO_DATE)
    temp = c.list_observations(end=end)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-CAL20170616-11-022,2017-06-19T03:21:29.345417+00'), \
        'wrong content'
    assert len(list(temp)) == 68, 'wrong count'

    temp = c.list_observations(end=end, maxrec=3)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GN-CAL20170616-11-022,2017-06-19T03:21:29.345417+00'), \
        'wrong content'
    assert len(list(temp)) == 2, 'wrong maxrec count'


@pytest.mark.skipif(not sys.version.startswith('3.6'),
                    reason='support 3.6 only')
def test_list_observations_start_end():
    start = datetime.strptime('2017-06-20T12:36:35.681662', ISO_DATE)
    end = datetime.strptime('2017-12-17T20:13:56.572387', ISO_DATE)
    temp = c.list_observations(start=start, end=end)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GS-CAL20150505-4-035,2017-06-20T21:56:27.86147+00'), \
        'wrong content'
    assert len(list(temp)) == 59, 'wrong count'

    temp = c.list_observations(start=start, end=end, maxrec=3)
    assert temp is not None, 'should have content'
    result = next(temp)
    assert result.startswith(
        'GS-CAL20150505-4-035,2017-06-20T21:56:27.86147+00'), \
        'wrong content'
    assert len(list(temp)) == 2, 'wrong maxrec count'


@pytest.mark.skipif(not sys.version.startswith('3.6'),
                    reason='support 3.6 only')
def test_parse_file_response():
    TEST_FILE = """
    [{"exposure_time": 60.0, "detector_roi_setting": "Fixed",
      "detector_welldepth_setting": "Deep",
      "telescope": "Gemini-North", "mdready": True,
      "requested_bg": 100, "engineering": False,
      "cass_rotator_pa": 255.832443162755,
      "ut_datetime": "2017-06-16 11:49:52.400000",
      "file_size": 2139030,
      "types": "["GNIRS", "LS", "GNIRS_LS", "GEMINI_NORTH", "GEMINI", "SIDEREAL", "GNIRS_SPECT", "SPECT", "RAW", "UNPREPARED"]",
      "requested_wv": 80, "detector_readspeed_setting": None,
      "size": 2139030, "laser_guide_star": False,
      "observation_id": "GN-2016B-Q-23-729",
      "science_verification": False, "raw_cc": 50,
      "filename": "N20170616S0268.fits.bz2",
      "instrument": "GNIRS", "reduction": "RAW",
      "camera": "LongRed", "ra": 314.121872675556,
      "detector_binning": "1x1",
      "lastmod": "2017-06-19 03:22:57.534578+00:00",
      "wavelength_band": "M", "data_size": 4204800,
      "mode": "LS", "raw_iq": 70, "airmass": 1.116,
      "elevation": 63.482995833333,
      "data_label": "GN-2016B-Q-23-729-001",
      "requested_iq": 70, "object": "WISE 2056",
      "requested_cc": 50, "program_id": "GN-2016B-Q-23",
      "file_md5": "1818e92d5ef710732c50ad1462743e0d",
      "central_wavelength": 4.8494, "raw_wv": 80,
      "compressed": True, "filter_name": "M",
      "detector_gain_setting": None, "path": "",
      "observation_class": "science", "qa_state": "Pass",
      "observation_type": "OBJECT",
      "calibration_program": False,
      "md5": "1818e92d5ef710732c50ad1462743e0d",
      "adaptive_optics": False, "name": "N20170616S0268.fits",
      "focal_plane_mask": "0.68arcsec",
      "data_md5": "2cd8507cafc4daa70d946ea92a7f8e7d",
      "raw_bg": 80, "disperser": "32_mm",
      "wavefront_sensor": "PWFS2", "gcal_lamp": None,
      "detector_readmode_setting": "Bright_Objects",
      "phot_standard": None, "local_time": "01:49:51.400000",
      "spectroscopy": True, "azimuth": 96.036230555556,
      "release": "2018-06-16", "dec": 14.999244792027}]
    """
    x = c._parse_file_response(json.loads(TEST_FILE))
    assert x is not None, 'should return a file value'

#     TEST_OBS = "[{'exposure_time': 59.6464, 'detector_roi_setting': 'Fixed', " \
#                "'detector_welldepth_setting': None, " \
#                "'telescope': 'Gemini-South', 'mdready': True, " \
#                "'requested_bg': None, 'engineering': False, " \
#                "'cass_rotator_pa': -90.000078709113, " \
#                "'ut_datetime': '2018-12-17 20:39:06.400000', " \
#                "'file_size': 11342657, " \
#                "'types': \"['GPI', 'IFU', 'NON_SIDEREAL', 'GEMINI', 'GEMINI_SOUTH', 'SPECT', 'GPI_SPECT', 'RAW', 'UNPREPARED']\"," \
#                "requested_wv': None, 'detector_readspeed_setting': None, " \
#                "'size': 11342657, 'laser_guide_star': False, " \
#                "'observation_id': 'GS-CAL20181216-1', " \
#                "'science_verification': False, 'raw_cc': None, " \
#                "'filename': 'S20181218S0031.fits.bz2', 'instrument': 'GPI', " \
#                "'reduction': 'RAW', 'camera': None, 'ra': 147.0, " \
#                "'detector_binning': '1x1', " \
#                "'lastmod': '2018-12-17 20:50:16.114343+00:00', " \
#                "'wavelength_band': None, 'data_size': 21006720, " \
#                "'mode': 'IFS', 'raw_iq': None, 'airmass': 1.0, " \
#                "'elevation': 90.049866666667, " \
#                "'data_label': 'GS-CAL20181216-1-069', " \
#                "'requested_iq': None, 'object': 'Dark', " \
#                "'requested_cc': None, 'program_id': 'GS-CAL20181216', " \
#                "'file_md5': '05921169f26708f2d974c5060d2809c9', " \
#                "'central_wavelength': None, 'raw_wv': None, " \
#                "'compressed': True, 'filter_name': 'H', " \
#                "'detector_gain_setting': None, 'path': '', " \
#                "'observation_class': 'dayCal', 'qa_state': 'Undefined', " \
#                "'observation_type': 'DARK', 'calibration_program': True, " \
#                "'md5': '05921169f26708f2d974c5060d2809c9', " \
#                "'adaptive_optics': False, 'name': 'S20181218S0031.fits', " \
#                "'focal_plane_mask': 'FPM_H', " \
#                "'data_md5': '009e773c1fe137e4db7d0b11388e883c', " \
#                "'raw_bg': None, 'disperser': 'DISP_PRISM', " \
#                "'wavefront_sensor': None, 'gcal_lamp': None, " \
#                "'detector_readmode_setting': 'None', 'phot_standard': None, " \
#                "'local_time': '17:39:05.900000', 'spectroscopy': True, " \
#                "'azimuth': 147.000020833333, 'release': '2018-12-17', " \
#                "'dec': 89.9}]"
#     y = c._parse_obs(TEST_OBS)
#
#     TEST_PROGRAM = "<!DOCTYPE html>" \
#                    "<html><head>" \
#                    "<meta charset=\"UTF-8\">" \
#                    ""
#                    "<link rel=\"stylesheet\" type=\"text/css\" href=\"/static/table.css\">" \
#                    "<title>Detail for Program: GN-2016B-Q-23</title>" \
# "<meta name=\"description\" content="">" \
#                         "</head>" \
# "<body>" \
# "<h1>Program: GN-2016B-Q-23</h1>" \
# "" \
# "<table>" \
# " <tr><td>Title:<td>A Thermal Infrared Spectroscopic Sequence of the Coldest Brown Dwarfs</tr>" \
# " <tr><td>PI:<td>Andrew Skemer</tr>" \
# " <tr><td>Co-I(s):<td> katelyn allers,  Michael Cushing,  Tom Geballe,  Gordon Bjoraker,  Caroline Morley,  Jackie Faherty,  Jonathan Fortney,  Mark Marley,  Adam Schneider</tr>" \
# "</table>" \
# "<h2>Abstract</h2>" \
# "<div style=\"max-width: 20cm\">" \
#                             "The coldest exoplanets and brown dwarfs emit most of their light through an atmospheric window at ~4-5 microns, just like Jupiter. But with JWST set to launch in 2 years, there are no published 5 micron spectra of extrasolar planets or brown dwarfs colder than 700 K.  Recently, our team obtained the first spectrum of  WISE 0855, a 250 K brown dwarf, using Gemini/GNIRS at 5 microns.  The spectrum reveals water vapor, clouds, and an overall appearance that is strikingly similar to Jupiter.  We are now proposing to extend the use of 5 micron spectroscopy to a sequence of brown dwarfs spanning 700 K to 250 K, with the goal of understanding how water vapor, clouds, and non-equilibrium chemistry vary with temperature.  Obtaining these observations now will allow us to refine our exoplanet and brown dwarf atmosphere models at thermal infrared wavelengths in time for JWST." \
# "</div>" \
# "" \
# "" \
# "" \
# "</body>" \
# "</html>"
#
