import numpy as np
import astropy.units as u
from astropy.time import Time, TimeDelta
from astropy.coordinates import CartesianRepresentation, SkyCoord, HeliocentricTrueEcliptic, ICRS
from astropy.constants import c

def approx_earth_heliocentric_position(time):
    # Orbital elements for Earth at J2000 epoch
    a = 1.000001018 * u.AU       # semi-major axis
    e = 0.0167086                # eccentricity
    M0 = np.deg2rad(357.51716)   # mean anomaly at J2000
    omega = np.deg2rad(102.9373) # argument of perihelion
    T0 = Time('J2000.0').tdb

    # Mean motion in rad/day
    n = 2 * np.pi / 365.256363004

    # Time in days since J2000
    t = (time.tdb - T0).to(u.day).value

    M = M0 + n * t

    # Solve Kepler's equation
    def kepler_eq(E, M, e):
        return E - e * np.sin(E) - M

    def solve_kepler(M, e):
        E = M
        for _ in range(5):
            E -= kepler_eq(E, M, e) / (1 - e * np.cos(E))
        return E

    E = solve_kepler(M, e)

    # True anomaly
    nu = 2 * np.arctan2(np.sqrt(1+e) * np.sin(E/2),
                        np.sqrt(1-e) * np.cos(E/2))

    # Radius vector
    r = a * (1 - e * np.cos(E))

    # Position in orbital plane (ecliptic coordinates)
    x = r * np.cos(omega + nu)
    y = r * np.sin(omega + nu)
    z = 0 * u.AU

    # Create CartesianRepresentation in HeliocentricTrueEcliptic frame
    pos_ecl = CartesianRepresentation(x, y, z)
    coord_ecl = SkyCoord(pos_ecl, frame=HeliocentricTrueEcliptic(equinox='J2000'))

    # Transform to ICRS (equatorial frame)
    coord_icrs = coord_ecl.transform_to(ICRS())

    return coord_icrs.cartesian


# ~few seconds difference from Time.light_travel_time, but much-much faster
def fast_light_travel_time_heliocentric_elliptical(time, target):
    earth_pos = approx_earth_heliocentric_position(time)
    los_unit_vec = target.icrs.represent_as('cartesian').get_xyz()
    los_unit_vec /= np.linalg.norm(los_unit_vec, axis=0)
    los_unit = CartesianRepresentation(los_unit_vec)

    projection = earth_pos.dot(los_unit)
    return TimeDelta(projection / c)