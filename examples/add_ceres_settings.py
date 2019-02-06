from mopro.database import initialize_database, CeresSettings, database

initialize_database()


with open('examples/ceres_12/rc_remplate.txt') as f:
    rc_template = f.read()

with open('examples/ceres_12/ceres_12_resources.tar.gz', 'rb') as f:
    resource_files = f.read()


# these are the settings 12 as defined by Fabian and Jens
with database.atomic():
    CeresSettings.insert(
        name='12_diffuse',
        revision=19439,
        rc_template=rc_template,
        resource_files=resource_files,
        off_target_distance=6,
        diffuse=True,
        psf_sigma=2.0,
        apd_dead_time=3.0,
        apd_recovery_time=8.75,
        apd_cross_talk=0.1,
        apd_afterpulse_probability_1=0.14,
        apd_afterpulse_probability_2=0.11,
        excess_noise=0.096,
        nsb_rate=None,
        additional_photon_acceptance=0.85,
        dark_count_rate=0.004,
        pulse_shape_function='(1.239*(1-1/(1+exp((0.5*x-2.851)/1.063)))*exp(-(0.5*x-2.851)/19.173))',
        residual_time_spread=0.0,
        gapd_time_jitter=1.5,
        discriminator_threshold=-192.387,
    ).execute()
