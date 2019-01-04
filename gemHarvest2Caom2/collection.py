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
    raise NotImplementedError('GET observation')
    file_url = ('https://archive.gemini.edu/jsonsummary/canonical/filepre=N20170616S0268')
    obs_url = ('https://archive.gemini.edu/jsonsummary/canonical/GS-CAL20181216-1-069')
    program_url = ('https://archive.gemini.edu/programinfo/GN-2016B-Q-23')
    import requests
    from requests.adapters import HTTPAdapter
    from requests.packages.urllib3.util.retry import Retry
    session = requests.Session()
    retry = Retry(total=10, read=10, connect=10, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=10)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    r = session.get(file_url, timeout=20)
    j = r.json()
    obs = _parse_file(j)
    r = session.get(obs_url, timeout=20)
    j = r.json()
    obs = _parse_obs(j, obs)
    r = session.get(program_url, timeout=20)

    obs = _parse_program()


def _parse_file_response(response):
    release = response['release']
    target = response['object']
    mode = response['mode']
    obstype = response['observation_type']
    obsclass = response['observation_class']
    reduction = response['reduction']
    program_id = response['program_id']
    # instrument = unicode(response['instrument'])
    # telescope = unicode(response['telescope'])
    instrument = response['instrument']
    telescope = response['telescope']
    return None

    if not response['data_label']:
        print('NULL data_label; processing skipped.\n')
        return(0)

    if not response['observation_id']:
        print('NULL observation ID; processing skipped.\n')
        return(0)

    if not release:
        print('NULL release date; processing skipped.\n')
        return(0)

    # One NIRI file has a bad release date that breaks things

    if release == '0001-03-31':
        release = '2008-03-31'

    # In a number of instances there are bad data labels or other
    # metadata...

    # At least one observation, GN-CAL20100323-1-901, has quotes around
    # the data label which breaks things.  Hence the 'strip' below...
    self.obs_id = unicode(obs['data_label']).strip('"')

    # Phoenix files with error in data label
    if 'GS-2002A-DD-1-?' in obs['data_label']:
        self.obs_id = obs['data_label'].replace('?', '11')

    # Bad data labels for some GMOS-S data
    if 'BIAS/MBIAS/G-BIAS' in obs['data_label']:
        self.obs_id = obs['data_label'].replace('BIAS/MBIAS/', '')
    if '/NET/PETROHUE/DATAFLOW/' in obs['data_label']:
        self.obs_id = obs['data_label'].replace('-/NET/PETROHUE/DATAFLOW/', '')
    if '/EXPORT/HOME/SOS2/PREIMAGE/MPROC/' in obs['data_label']:
        self.obs_id = obs['data_label'].replace('/EXPORT/HOME/SOS2/PREIMAGE/MPROC/', '')

    # OBJECT string too long in this file...
    # 'NGC6823Now exiting seqexec and restarting seqexec.dev -simtcs and lo'
    if self.obs_id == 'GN-2002A-SV-78-68-007':
        target = 'NGC6823'

    fits_name = obs['filename'].replace('.bz2', '')
    print('File name: {}  Instrument: {}  Obs. ID: {}'.format(fits_name,
                                                              instrument, self.obs_id))

    release_date = datetime.strptime(unicode(release),
                                     '%Y-%m-%d')

    if 'North' in telescope:
        geo_location = geolocation(-155.46906, 19.823806, 4213.0)
    else:
        geo_location = geolocation(-70.736693, -30.240750, 2722.0)

        xmlfile = self.obs_id + '.xml'
        xmlfh = open(xmlfile, 'w')

        # Create a plane for the observation
        pln = caom2.Plane(self.obs_id)

        self.client = CAOM2RepoClient(net.Subject(netrc=True),
                                      resource_id= u'ivo://cadc.nrc.ca/sc2repo')

        # If the observation already exists in CAOM2 tables read it
        # so that it can be updated.
        try:
            self.obs = self.client.get_observation(self.collection, self.obs_id)
            self.obs_status = 'update'
        except cadcutils.exceptions.NotFoundException as e:
            # If the observation doesn't exist create it.
            self.obs = caom2.SimpleObservation(self.collection, self.obs_id)
            self.obs_status = 'create'

        self.obs.instrument = caom2.Instrument(instrument)
        self.obs.meta_release = release_date
        if obstype:
            self.obs.type = unicode(obstype)
            if obstype == 'MASK':
                if 'GN' in filename:
                    telescope = unicode('Gemini-North')
                else:
                    telescope = unicode('Gemini-South')

        if target:
            self.obs.target = caom2.Target(target)
        if obsclass == 'science':
            self.obs.intent = caom2.ObservationIntentType.SCIENCE
        elif not obsclass:
            self.obs.intent = None
        else:
            self.obs.intent = caom2.ObservationIntentType.CALIBRATION
        self.obs.proposal = caom2.Proposal(program_id)
        self.obs.telescope = caom2.observation.Telescope(telescope,
                                                         float(geo_location[0]),
                                                         float(geo_location[1]),
                                                         float(geo_location[2]))


        # Create a plane for the observation.
        # I'm setting the provenance 'reference' to a link that will take the
        # user directly to the Gemini archive query result for the observation
        # in question.

        pln.provenance = caom2.Provenance(name=u'Gemini Observatory Data',
                                          producer = u'Gemini Observatory', project = u'Gemini Archive',
                                          reference = u'http://archive.gemini.edu/searchform/' +
                                                      self.obs_id)
        pln.data_release = release_date
        pln.meta_release = release_date

        if mode == 'imaging' or obstype == 'MASK':
            pln.data_product_type = caom2.DataProductType.IMAGE
        else:
            pln.data_product_type = caom2.DataProductType.SPECTRUM

        if reduction == 'RAW':
            pln.calibration_level = caom2.CalibrationLevel.RAW_STANDARD
        else:
            pln.calibration_level = caom2.CalibrationLevel.CALIBRATED

        # Create an artifact for the FITS file and add the part to the artifact

        content_type = 'application/fits'
        file_size = obs['data_size']
        checksum = caom2.common.ChecksumURI('md5:' + obs['data_md5'])
        if obstype == 'MASK':
            product_type = caom2.ProductType.AUXILIARY
        else:
            product_type = caom2.ProductType.SCIENCE
        uri = u'gemini:GEM/{}'.format(fits_name)
        art = caom2.Artifact(uri, product_type, caom2.ReleaseType.DATA,
                             content_length=file_size,
                             content_type=unicode(content_type),
                             content_checksum=checksum)
        pln.artifacts.add(art)

        # Create an artifact for the associated preview

        content_type = 'image/jpeg'
        file_size = None
        checksum = None
        product_type = caom2.ProductType.PREVIEW
        preview_name = fits_name.replace('.fits', '.jpg')
        uri = u'gemini:GEM/{}'.format(preview_name)
        art = caom2.Artifact(uri, product_type, caom2.ReleaseType.DATA,
                             content_length=file_size, content_type=unicode(content_type),
                             content_checksum=checksum)
        pln.artifacts.add(art)

        # Finally, add the plane to the observation

        self.obs.planes.add(pln)

        # Write the XML output to the appropriate file to check
        # that the format is valid...

        obs_rw.ObservationWriter(validate=True).write(self.obs, out=xmlfile)
        xmlfh.close()
        os.remove(xmlfile)

        # Put the observation in repository...
        self.caom2repo()


def _parse_obs(obs_stuff):
    instrument = obs_stuff['instrument']
    obsclass = obs_stuff['observation_class']
    obstype = obs_stuff['observation_type']
    mode = obs_stuff['mode']
    spectroscopy = obs_stuff['spectroscopy']
    program_id = obs_stuff['program_id']
    bandpassname = obs_stuff['wavelength_band']
    filter_name = obs_stuff['filter_name']
    types = obs_stuff['types']
    target = unicode(obs_stuff['object'])
    ra = obs_stuff['ra']
    dec = obs_stuff['dec']
    ut_date_time = unicode(obs_stuff['ut_datetime'])
    exptime = obs_stuff['exposure_time']
    time_crpix = 0.5

    # Determine energy band based on instrument name

    if instrument in ['GMOS-N', 'GMOS-S', 'GRACES', 'bHROS', 'hrwfs']:
        energy_band = caom2.EnergyBand['OPTICAL']
    else:
        energy_band = caom2.EnergyBand['INFRARED']

    # At least one dark observation had a null exposure time in metadata
    # (FITS value was a large -ve number!). Setting to zero to get
    # something ingested.

    if not exptime:
        exptime = 0.0

    if 'AZEL_TARGET' in types:
        # Az-El coordinate frame so no spatial WCS info.
        azel = True
    else:
        azel = None

    # Fetch the CAOM2 observation
    observation = self.repo_client.get_observation(self.collection,
                                                   self.obsID)

    if 'NON_SIDEREAL' in types:
        # Non-sidereal tracking -> setting moving target to "True"
        moving_target = True
    else:
        moving_target = None

    if spectroscopy:
        observation.target = caom2.Target(target, target_type=u'object',
                                          moving=moving_target)
    else:
        observation.target = caom2.Target(target, target_type=u'field',
                                          moving=moving_target)

    observation.proposal = caom2.Proposal(program_id)


def _parse_program(program):
    # program = r.text
    soup = BeautifulSoup(program, 'lxml')
    tds = soup.find_all('td')
    if len(tds) > 0:
        title = tds[1].contents[0].replace('\n', ' ')
        pi_name = tds[3].contents[0]
        observation.proposal.pi_name = pi_name
        observation.proposal.title = title
    r.close

    # Early Gemini data did not have an OBSCLASS keyword.  Try to
    # determine a value from the OBSTYPE keyword as well as the
    # program ID.  Assume that caom2.Observation.intent is CALIBRATION
    # if OBSCLASS is unknown and OBSTYPE is not OBJECT.

    if not obsclass:
        if obstype == 'OBJECT':
            if 'CAL' not in program_id:
                observation.intent = caom2.ObservationIntentType.SCIENCE
            else:
                observation.intent = caom2.ObservationIntentType.CALIBRATION
        else:
            observation.intent = caom2.ObservationIntentType.CALIBRATION
    elif obsclass == 'science':
        observation.intent = caom2.ObservationIntentType.SCIENCE
    else:
        observation.intent = caom2.ObservationIntentType.CALIBRATION

    for plane in observation.planes.values():

        # Add temporal information to the plane, assuming this
        # will be the same for each plane for the time being.
        # Approximate the polygon for the temporal information as
        # an interval derived from start/stop exposure times.

        time_ref_coord = caom2.RefCoord(time_crpix, str2mjd(ut_date_time))
        time_cf = caom2.CoordFunction1D(long(1), exptime/(3600.0*24.0),
                                        time_ref_coord)
        time = caom2.TemporalWCS(axis=caom2.CoordAxis1D(caom2.Axis(u'TIME',
                                                                   u'd'), function=time_cf), timesys=u'UTC',
                                 trefpos=u'TOPOCENTER', exposure=exptime, resolution=exptime)
        dim = 1
        start = str2mjd(ut_date_time)
        stop = start + exptime/(3600.0 * 24.0)
        bounds = caom2.shape.Interval(start, stop,
                                      [caom2.shape.SubInterval(start, stop)])
        plane.time = caom2.plane.Time(bounds=bounds, dimension=dim,
                                      resolution=exptime, sample_size=exptime/(3600.0 * 24.0),
                                      exposure=exptime)

        # Add spatial information if NOT AZ-EL coordinate system
        # and there is a valid RA value.  For now, assume a small
        # 0.001 degree square footprint centered on the RA/Dec values
        # provided in Gemini metadata

        if not azel and ra:
            points = []
            vertices = []
            segment_type = caom2.SegmentType['MOVE']
            #for x, y in ([0, 0], [1, 0], [1, 1], [0, 1 ]):
            for x, y in ([0, 1], [1, 1], [1, 0], [0, 0 ]):
                ra_pt = ra - 0.001*(0.5-float(x))
                dec_pt = dec - 0.001*(0.5-float(y))
                #ra_pt = ra - 0.001*float(x)
                #dec_pt = dec - 0.001*float(y)
                points.append(caom2.shape.Point(ra_pt,dec_pt))
                vertices.append(caom2.shape.Vertex(ra_pt,dec_pt,segment_type))
                segment_type = caom2.SegmentType['LINE']
            vertices.append(caom2.shape.Vertex(ra,dec,
                                               caom2.SegmentType['CLOSE']))
            polygon = caom2.shape.Polygon(points=points,
                                          samples=caom2.shape.MultiPolygon(vertices))
            position = caom2.plane.Position(time_dependent=moving_target, bounds=polygon)
            plane.position = position


        # Add what energy information Gemini metadata provides to the
        # plane, again assuming this is the same for each plane for
        # the time being.

        plane.energy = caom2.plane.Energy()
        plane.energy.em_band = energy_band
        plane.energy.bandpass_name = filter_name

    obs_rw.ObservationWriter(validate=True).write(observation, out=xmlfile)
    xmlfh.close()
    #os.remove(xmlfile)

    # Put the observation into the repository...
    self.caom2repo(observation)


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
