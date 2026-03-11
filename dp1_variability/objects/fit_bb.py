from functools import cache

import numpy as np
from astropy import units as u
from astropy.modeling.physical_models import BlackBody
from astropy.table import Table
from scipy.optimize import least_squares
from upath import UPath

LSST_THROUGHPUT_BASE_URL = UPath("https://github.com/lsst/throughputs/raw/refs/heads/main/baseline/")

def normalize_throughput(t):
    """Normalize the throughput table."""
    integrand = u.Quantity(t["throughput"] / t["wavelength"])
    t["normalized_throughput"] = integrand / np.trapezoid(y=integrand.cgs.value, x=u.Quantity(t["wavelength"]).cgs.value)
    return t

@cache
def get_lsst_throughput(band):
    """Get the LSST throughput for a given band."""
    url = LSST_THROUGHPUT_BASE_URL / f"total_{band}.dat"
    t = Table.read(str(url), format="ascii", names=["wavelength", "throughput"])
    t["wavelength"] *= u.nm
    return normalize_throughput(t)

SVO_BASE_URL = "https://svo2.cab.inta-csic.es/svo/theory/fps3/getdata.php?format=ascii&id="

@cache
def get_throughput_from_svo(band_spec):
    """Get the throughput for a given band from SVO."""
    url = SVO_BASE_URL + band_spec
    t = Table.read(url, format="ascii", names=["wavelength", "throughput"])
    t["wavelength"] *= u.AA
    return normalize_throughput(t)

@cache
def get_decam_throughput(band):
    """Get the DECam throughput for a given band."""
    return get_throughput_from_svo(f"CTIO/DECam.{band}")

@cache
def get_gaia_throughput(band):
    if band != "G":
        band = f"G{band.lower()}"
    return get_throughput_from_svo(f"GAIA/GAIA3.{band}")

@cache
def get_throughput(band):
    if band.startswith("DECam."):
        return get_decam_throughput(band.removeprefix("DECam."))
    if band in ["G", "BP", "RP"]:
        return get_gaia_throughput(band)
    if len(band) == 1:
        return get_lsst_throughput(band)
    raise ValueError(f"Unknown band: {band}")


def black_body_flux(temperature, band, solid_angle=1.0 * u.steradian):
    """Calculate the flux of a black body at a given temperature in a specific band."""
    throughput = get_throughput(band)
    waves = u.Quantity(throughput["wavelength"])
    norm_throughput = u.Quantity(throughput["normalized_throughput"])
    bb = BlackBody(temperature=temperature)
    bb_flux = bb(waves)
    fluxband = np.trapezoid(y=bb_flux.cgs.value * norm_throughput.cgs.value, x=waves.cgs.value) * bb_flux.unit
    return fluxband * solid_angle


def black_body_magn(temperature, band, solid_angle):
    """Calculate the magnitude of a black body at a given temperature in a specific band."""
    flux = black_body_flux(temperature, band, solid_angle)
    return u.Magnitude(flux).to(u.ABmag).value


def fit_mags(bands, mags, errors, *, T_init=7000, lg_solid_angle_init = np.log10(((1.0 * u.R_sun) / (10*u.kpc)).cgs.value**2)):
    def residuals(params):
        """Calculate residuals for the least squares fit."""
        temperature, lg_solid_angle = params
        solid_angle = 10**lg_solid_angle * u.steradian
        return [
            (black_body_magn(temperature*u.K, band, solid_angle=solid_angle) - mag) / err
            for band, mag, err in zip(bands, mags, errors, strict=True)
        ]

    return least_squares(
        residuals,
        [T_init, lg_solid_angle_init],
    )