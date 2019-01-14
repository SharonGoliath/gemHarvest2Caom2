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
import requests

from bs4 import BeautifulSoup
from datetime import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from caom2 import SimpleObservation, Plane, Instrument, Target
from caom2 import ObservationIntentType, Proposal, Telescope, EnergyBand
from caom2 import Provenance, DataProductType, CalibrationLevel
from caom2 import ProductType, ChecksumURI, ReleaseType, Artifact
from caom2 import RefCoord, shape, SegmentType, Position, Energy
from caom2 import Time as caom2_Time

from caom2pipe import astro_composable as ac
from caom2pipe import manage_composable as mc

COLLECTION = 'GEMINI'  # name of CAOM2 collection
SCHEME = 'gemini'
ARCHIVE = 'GEM'

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
        yield '{}\n'.format(','.join(ii))


def get_observation(id):
    """
    Return the observation corresponding to the id
    :param id: id of the observation
    :return: observation corresponding to the id or None if such
    such observation does not exist
    """
    file_url = 'https://archive.gemini.edu/jsonsummary/canonical/filepre=N20170616S0268'
    obs_url = 'https://archive.gemini.edu/jsonsummary/canonical/GS-CAL20181216-1-069'
    program_url = 'https://archive.gemini.edu/programinfo/GN-2016B-Q-23'

    # response = _query_endpoint(file_url)
    # json_response = response.json()
    # response.close
    # obs = _parse_file_response(json_response, id)
    #
    # response = _query_endpoint(obs_url)
    # json_response = response.json()
    # response.close
    # obs = _parse_obs(json_response, obs)
    #
    # response = _query_endpoint(program_url)
    # obs = _parse_program(response.text, obs)
    # response.close
    #

    response = _query_endpoint(obs_url)
    json_response = response.json()
    response.close
    obs = _parse_obs_2(json_response, id)

    response = _query_endpoint(program_url)
    obs = _parse_program(response.text, obs)
    response.close
    return obs


def _query_endpoint(endpoint_url):
    session = requests.Session()
    retry = Retry(total=10, read=10, connect=10, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    r = session.get(endpoint_url, timeout=20)
    return r


def _parse_obs_2(response, obs_id):
    data_label = _response_lookup(response, 'data_label')
    if data_label is None:
        logger.error(
            'NULL data_label: processing skipped for {}'.format(obs_id))
        return None

    obs = SimpleObservation(collection=COLLECTION,
                            observation_id=response['data_label'])

    release = _response_lookup(response, 'release')
    target = _response_lookup(response, 'object')
    mode = _response_lookup(response, 'mode')
    reduction = _response_lookup(response, 'reduction')
    telescope = _response_lookup(response, 'telescope')
    instrument = _response_lookup(response, 'instrument')
    obsclass = _response_lookup(response, 'observation_class')
    obstype = _response_lookup(response, 'observation_type')
    spectroscopy = _response_lookup(response, 'spectroscopy')
    program_id = _response_lookup(response, 'program_id')
    filter_name = _response_lookup(response, 'filter_name')
    types = _response_lookup(response, 'types')
    ra = _response_lookup(response, 'ra')
    dec = _response_lookup(response, 'dec')
    ut_date_time = _response_lookup(response, 'ut_datetime')
    exptime = _response_lookup(response, 'exposure_time')
    file_name = _response_lookup(response, 'filename')

    # One NIRI file has a bad release date that breaks things
    release_date = None
    if release is not None:
        if release == '0001-03-31':
            release = '2008-03-31'
        release_date = datetime.strptime(release, '%Y-%m-%d')

    # In a number of instances there are bad data labels or other
    # metadata...

    # At least one observation, GN-CAL20100323-1-901, has quotes around
    # the data label which breaks things.  Hence the 'strip' below...
    obs.observation_id = obs.observation_id.strip('"')

    # Phoenix files with error in data label
    if 'GS-2002A-DD-1-?' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace('?', '11')

    # Bad data labels for some GMOS-S data
    if 'BIAS/MBIAS/G-BIAS' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace('BIAS/MBIAS/', '')
    if '/NET/PETROHUE/DATAFLOW/' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace(
            '-/NET/PETROHUE/DATAFLOW/', '')
    if '/EXPORT/HOME/SOS2/PREIMAGE/MPROC/' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace(
            '/EXPORT/HOME/SOS2/PREIMAGE/MPROC/', '')

    # OBJECT string too long in this file...
    # 'NGC6823Now exiting seqexec and restarting seqexec.dev -simtcs and lo'
    if obs.observation_id == 'GN-2002A-SV-78-68-007':
        target = 'NGC6823'

    fits_name = response['filename'].replace('.bz2', '')

    # Create a plane for the observation
    pln = Plane(obs.observation_id)

    # Determine energy band based on instrument name

    if (instrument is not None and instrument in ['GMOS-N', 'GMOS-S', 'GRACES',
                                                  'bHROS', 'hrwfs']):
        energy_band = EnergyBand['OPTICAL']
        obs.instrument = Instrument(instrument)
    else:
        energy_band = EnergyBand['INFRARED']

    obs.meta_release = release_date
    if obstype is not None:
        obs.type = obstype
        if obstype == 'MASK':
            if 'GN' in file_name:
                telescope = 'Gemini-North'
            else:
                telescope = 'Gemini-South'

    if telescope is not None and 'North' in telescope:
        x, y, z = ac.get_location(19.823806, -155.46906, 4213.0)
    else:
        x, y, z = ac.get_location(-30.240750, -70.736693, 2722.0)

    if 'NON_SIDEREAL' in types:
        # Non-sidereal tracking -> setting moving target to "True"
        moving_target = True
    else:
        moving_target = None
    if target is not None and spectroscopy is not None:
        if spectroscopy:
            obs.target = Target(target, target_type='object',
                            moving=moving_target)
        else:
            obs.target = Target(target, target_type='field',
                            moving=moving_target)

    if obsclass is None:
        obs.intent = None
    elif obsclass == 'science':
        obs.intent = ObservationIntentType.SCIENCE
    else:
        obs.intent = ObservationIntentType.CALIBRATION
    obs.proposal = Proposal(program_id)
    obs.telescope = Telescope(telescope, x, y, z)

    # Create a plane for the observation.
    # I'm setting the provenance 'reference' to a link that will take the
    # user directly to the Gemini archive query result for the observation
    # in question.

    pln.provenance = Provenance(name='Gemini Observatory Data',
                                producer='Gemini Observatory',
                                project='Gemini Archive',
                                reference='http://archive.gemini.edu/searchform/' +
                                          obs.observation_id)
    pln.data_release = release_date
    pln.meta_release = release_date

    if ((mode is not None and mode == 'imaging') or
            (obstype is not None and obstype == 'MASK')):
        pln.data_product_type = DataProductType.IMAGE
    else:
        pln.data_product_type = DataProductType.SPECTRUM

    if reduction is not None and reduction == 'RAW':
        pln.calibration_level = CalibrationLevel.RAW_STANDARD
    else:
        pln.calibration_level = CalibrationLevel.CALIBRATED

    # Create an artifact for the FITS file and add the part to the artifact

    content_type = 'application/fits'
    file_size = response['data_size']
    checksum = ChecksumURI('md5:' + response['data_md5'])
    if obstype == 'MASK':
        product_type = ProductType.AUXILIARY
    else:
        product_type = ProductType.SCIENCE
    uri = mc.build_uri(ARCHIVE, fits_name, SCHEME)
    art = Artifact(uri, product_type, ReleaseType.DATA,
                   content_length=file_size,
                   content_type=content_type,
                   content_checksum=checksum)
    pln.artifacts.add(art)

    # Create an artifact for the associated preview

    content_type = 'image/jpeg'
    file_size = None
    checksum = None
    product_type = ProductType.PREVIEW
    preview_name = fits_name.replace('.fits', '.jpg')
    uri = mc.build_uri(ARCHIVE, preview_name, SCHEME)
    art = Artifact(uri, product_type, ReleaseType.DATA,
                   content_length=file_size, content_type=content_type,
                   content_checksum=checksum)
    pln.artifacts.add(art)

    # Finally, add the plane to the observation
    obs.planes.add(pln)

    # At least one dark observation had a null exposure time in metadata
    # (FITS value was a large -ve number!). Setting to zero to get
    # something ingested.

    if exptime is None:
        exptime = 0.0

    if 'AZEL_TARGET' in types:
        # Az-El coordinate frame so no spatial WCS info.
        azel = True
    else:
        azel = None

    for plane in obs.planes.values():

        # Add temporal information to the plane, assuming this
        # will be the same for each plane for the time being.
        # Approximate the polygon for the temporal information as
        # an interval derived from start/stop exposure times.

        dim = 1
        start = ac.get_datetime(ut_date_time).value
        stop = start + exptime/(3600.0 * 24.0)
        bounds = shape.Interval(start, stop,
                                [shape.SubInterval(start, stop)])
        plane.time = caom2_Time(bounds=bounds, dimension=dim,
                                resolution=exptime,
                                sample_size=exptime / (3600.0 * 24.0),
                                exposure=exptime)

        # Add spatial information if NOT AZ-EL coordinate system
        # and there is a valid RA value.  For now, assume a small
        # 0.001 degree square footprint centered on the RA/Dec values
        # provided in Gemini metadata

        if not azel and ra:
            points = []
            vertices = []
            segment_type = SegmentType['MOVE']
            for x, y in ([0, 1], [1, 1], [1, 0], [0, 0 ]):
                ra_pt = ra - 0.001*(0.5-float(x))
                dec_pt = dec - 0.001*(0.5-float(y))
                points.append(shape.Point(ra_pt,dec_pt))
                vertices.append(shape.Vertex(ra_pt,dec_pt,segment_type))
                segment_type = SegmentType['LINE']
            vertices.append(shape.Vertex(ra, dec,
                                         SegmentType['CLOSE']))
            polygon = shape.Polygon(points=points,
                                    samples=shape.MultiPolygon(vertices))
            position = Position(time_dependent=moving_target,
                                bounds=polygon)
            plane.position = position


        # Add what energy information Gemini metadata provides to the
        # plane, again assuming this is the same for each plane for
        # the time being.

        plane.energy = Energy()
        plane.energy.em_band = energy_band
        plane.energy.bandpass_name = filter_name

    return obs


def _parse_file_response(response, file_name):

    data_label = _response_lookup(response, 'data_label')
    if data_label is None:
        logger.error(
            'NULL data_label: processing skipped for {}'.format(file_name))
        return None

    obs = SimpleObservation(collection=COLLECTION,
                            observation_id=response['data_label'])

    release = _response_lookup(response, 'release')
    target = _response_lookup(response, 'object')
    mode = _response_lookup(response, 'mode')
    obstype = _response_lookup(response, 'observation_type')
    obsclass = _response_lookup(response, 'observation_class')
    reduction = _response_lookup(response, 'reduction')
    program_id = _response_lookup(response, 'program_id')
    instrument = _response_lookup(response, 'instrument')
    telescope = _response_lookup(response, 'telescope')

    # One NIRI file has a bad release date that breaks things
    release_date = None
    if release is not None:
        if release == '0001-03-31':
            release = '2008-03-31'
        release_date = datetime.strptime(release, '%Y-%m-%d')

    # In a number of instances there are bad data labels or other
    # metadata...

    # At least one observation, GN-CAL20100323-1-901, has quotes around
    # the data label which breaks things.  Hence the 'strip' below...
    obs.observation_id = obs.observation_id.strip('"')

    # Phoenix files with error in data label
    if 'GS-2002A-DD-1-?' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace('?', '11')

    # Bad data labels for some GMOS-S data
    if 'BIAS/MBIAS/G-BIAS' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace('BIAS/MBIAS/', '')
    if '/NET/PETROHUE/DATAFLOW/' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace(
            '-/NET/PETROHUE/DATAFLOW/', '')
    if '/EXPORT/HOME/SOS2/PREIMAGE/MPROC/' in obs.observation_id:
        obs.observation_id = obs.observation_id.replace(
            '/EXPORT/HOME/SOS2/PREIMAGE/MPROC/', '')

    # OBJECT string too long in this file...
    # 'NGC6823Now exiting seqexec and restarting seqexec.dev -simtcs and lo'
    if obs.observation_id == 'GN-2002A-SV-78-68-007':
        target = 'NGC6823'

    fits_name = response['filename'].replace('.bz2', '')

    # Create a plane for the observation
    pln = Plane(obs.observation_id)

    obs.instrument = Instrument(instrument)
    obs.meta_release = release_date
    if obstype is not None:
        obs.type = obstype
        if obstype == 'MASK':
            if 'GN' in file_name:
                telescope = 'Gemini-North'
            else:
                telescope = 'Gemini-South'

    if telescope is not None and 'North' in telescope:
        # geo_location = geolocation(-155.46906, 19.823806, 4213.0)
        x, y, z = ac.get_location(19.823806, -155.46906, 4213.0)
    else:
        # geo_location = geolocation(-70.736693, -30.240750, 2722.0)
        x, y, z = ac.get_location(-30.240750, -70.736693, 2722.0)

    if target is not None:
        obs.target = Target(target)
    if obsclass is None:
        obs.intent = None
    elif obsclass == 'science':
        obs.intent = ObservationIntentType.SCIENCE
    else:
        obs.intent = ObservationIntentType.CALIBRATION
    obs.proposal = Proposal(program_id)
    obs.telescope = Telescope(telescope, x, y, z)

    # Create a plane for the observation.
    # I'm setting the provenance 'reference' to a link that will take the
    # user directly to the Gemini archive query result for the observation
    # in question.

    pln.provenance = Provenance(name='Gemini Observatory Data',
                                producer='Gemini Observatory',
                                project='Gemini Archive',
                                reference='http://archive.gemini.edu/searchform/' +
                                          obs.observation_id)
    pln.data_release = release_date
    pln.meta_release = release_date

    if ((mode is not None and mode == 'imaging') or
            (obstype is not None and obstype == 'MASK')):
        pln.data_product_type = DataProductType.IMAGE
    else:
        pln.data_product_type = DataProductType.SPECTRUM

    if reduction is not None and reduction == 'RAW':
        pln.calibration_level = CalibrationLevel.RAW_STANDARD
    else:
        pln.calibration_level = CalibrationLevel.CALIBRATED

    # Create an artifact for the FITS file and add the part to the artifact

    content_type = 'application/fits'
    file_size = response['data_size']
    checksum = ChecksumURI('md5:' + response['data_md5'])
    if obstype == 'MASK':
        product_type = ProductType.AUXILIARY
    else:
        product_type = ProductType.SCIENCE
    uri = mc.build_uri(ARCHIVE, fits_name, SCHEME)
    art = Artifact(uri, product_type, ReleaseType.DATA,
                   content_length=file_size,
                   content_type=content_type,
                   content_checksum=checksum)
    pln.artifacts.add(art)

    # Create an artifact for the associated preview

    content_type = 'image/jpeg'
    file_size = None
    checksum = None
    product_type = ProductType.PREVIEW
    preview_name = fits_name.replace('.fits', '.jpg')
    uri = mc.build_uri(ARCHIVE, preview_name, SCHEME)
    art = Artifact(uri, product_type, ReleaseType.DATA,
                   content_length=file_size, content_type=content_type,
                   content_checksum=checksum)
    pln.artifacts.add(art)

    # Finally, add the plane to the observation
    obs.planes.add(pln)
    return obs


def _response_lookup(response, lookup):
    result = None
    if lookup in response:
        result = response[lookup]
    return result


def _parse_obs(response, observation):
    instrument = _response_lookup(response, 'instrument')
    obsclass = _response_lookup(response, 'observation_class')
    obstype = _response_lookup(response, 'observation_type')
    spectroscopy = _response_lookup(response, 'spectroscopy')
    program_id = _response_lookup(response, 'program_id')
    filter_name = _response_lookup(response, 'filter_name')
    types = _response_lookup(response, 'types')
    target = _response_lookup(response, 'object')
    ra = _response_lookup(response, 'ra')
    dec = _response_lookup(response, 'dec')
    ut_date_time = _response_lookup(response, 'ut_datetime')
    exptime = _response_lookup(response, 'exposure_time')

    # Determine energy band based on instrument name

    if (instrument is not None and instrument in ['GMOS-N', 'GMOS-S', 'GRACES',
                                                  'bHROS', 'hrwfs']):
        energy_band = EnergyBand['OPTICAL']
    else:
        energy_band = EnergyBand['INFRARED']

    # At least one dark observation had a null exposure time in metadata
    # (FITS value was a large -ve number!). Setting to zero to get
    # something ingested.

    if exptime is None:
        exptime = 0.0

    if 'AZEL_TARGET' in types:
        # Az-El coordinate frame so no spatial WCS info.
        azel = True
    else:
        azel = None

    if 'NON_SIDEREAL' in types:
        # Non-sidereal tracking -> setting moving target to "True"
        moving_target = True
    else:
        moving_target = None

    if spectroscopy is not None and spectroscopy:
        observation.target = Target(target, target_type='object',
                                    moving=moving_target)
    else:
        observation.target = Target(target, target_type='field',
                                    moving=moving_target)

    observation.proposal = Proposal(program_id)


    # Early Gemini data did not have an OBSCLASS keyword.  Try to
    # determine a value from the OBSTYPE keyword as well as the
    # program ID.  Assume that caom2.Observation.intent is CALIBRATION
    # if OBSCLASS is unknown and OBSTYPE is not OBJECT.

    if not obsclass:
        if obstype == 'OBJECT':
            if 'CAL' not in program_id:
                observation.intent = ObservationIntentType.SCIENCE
            else:
                observation.intent = ObservationIntentType.CALIBRATION
        else:
            observation.intent = ObservationIntentType.CALIBRATION
    elif obsclass == 'science':
        observation.intent = ObservationIntentType.SCIENCE
    else:
        observation.intent = ObservationIntentType.CALIBRATION

    for plane in observation.planes.values():

        # Add temporal information to the plane, assuming this
        # will be the same for each plane for the time being.
        # Approximate the polygon for the temporal information as
        # an interval derived from start/stop exposure times.

        dim = 1
        start = ac.get_datetime(ut_date_time).value
        stop = start + exptime/(3600.0 * 24.0)
        bounds = shape.Interval(start, stop,
                                [shape.SubInterval(start, stop)])
        plane.time = caom2_Time(bounds=bounds, dimension=dim,
                                resolution=exptime,
                                sample_size=exptime / (3600.0 * 24.0),
                                exposure=exptime)

        # Add spatial information if NOT AZ-EL coordinate system
        # and there is a valid RA value.  For now, assume a small
        # 0.001 degree square footprint centered on the RA/Dec values
        # provided in Gemini metadata

        if not azel and ra:
            points = []
            vertices = []
            segment_type = SegmentType['MOVE']
            for x, y in ([0, 1], [1, 1], [1, 0], [0, 0 ]):
                ra_pt = ra - 0.001*(0.5-float(x))
                dec_pt = dec - 0.001*(0.5-float(y))
                points.append(shape.Point(ra_pt,dec_pt))
                vertices.append(shape.Vertex(ra_pt,dec_pt,segment_type))
                segment_type = SegmentType['LINE']
            vertices.append(shape.Vertex(ra, dec,
                                         SegmentType['CLOSE']))
            polygon = shape.Polygon(points=points,
                                    samples=shape.MultiPolygon(vertices))
            position = Position(time_dependent=moving_target,
                                bounds=polygon)
            plane.position = position


        # Add what energy information Gemini metadata provides to the
        # plane, again assuming this is the same for each plane for
        # the time being.

        plane.energy = Energy()
        plane.energy.em_band = energy_band
        plane.energy.bandpass_name = filter_name

    return observation


def _parse_program(program, observation):
    soup = BeautifulSoup(program, 'lxml')
    tds = soup.find_all('td')
    if len(tds) > 0:
        title = tds[1].contents[0].replace('\n', ' ')
        pi_name = tds[3].contents[0]
        observation.proposal.pi_name = pi_name
        observation.proposal.title = title
    return observation


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
    logger.debug('Observation list initialized in memory.')


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
                if len(temp) > 1 and 'data_label' not in row:
                    time_string = temp[3].strip().replace(' ', 'T')
                    results.append([temp[0].strip(), time_string])
    except Exception as e:
        logging.error('Could not read from csv file {}'.format(fqn))
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
    return [observation_list[x] for x in observation_list if
            (start_s <= x <= end_s)]
