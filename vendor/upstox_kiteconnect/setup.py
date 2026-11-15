from setuptools import setup

setup(
    name="kiteconnect",  # installs as the `kiteconnect` package, replacing pykiteconnect
    version="4.1.0+upstox.1",
    description="Kite Connect compatible client backed by the Upstox API",
    packages=["kiteconnect"],
    python_requires=">=3.10",
    install_requires=[
        "requests>=2.28",
        "python-dateutil>=2.8",
        "websocket-client>=1.6",
        "protobuf>=4.21",
        "upstox-python-sdk>=2.20",   # only its protobuf feed definitions are used
        "autobahn[twisted]>=23.1",   # imported by common_lib's legacy reconnect path
    ],
    license="MIT",
)
