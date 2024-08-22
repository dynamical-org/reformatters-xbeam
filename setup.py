import setuptools

setuptools.setup(
    name="dynamical_reformatters",
    version="0.1.0",
    install_requires=[
        "apache-beam[gcp]",
        "cfgrib",
        "eccodes",
        "xarray[io,accel,parallel,viz]",
        "xarray-beam",
        "rioxarray",
    ],
    packages=setuptools.find_packages(),
)
