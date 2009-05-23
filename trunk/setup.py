try:
    from setuptools import setup
    from setuptools.command.bdist_egg import bdist_egg
except ImportError:
    print 'fall back'
    from distutils.core import setup

readme_text = file('README', 'rb').read()

setup(name ="papy",
      version ="1.0b",
      description ="Parallel Pipelines for Python",
      long_description =readme_text,
      license ="GPL",
      keywords ="multiprocessing parallel pipeline workflow rpc rpyc\
      multithreading",
      author ="Marcin Cieslik",
      author_email ="marcin.cieslik@gmail.com",
      url ="http://muralab.org/PaPy",
      packages =['papy', 'IMap', 'papy.workers', 'papy.utils'],
      package_dir ={'papy': 'src/papy', 'IMap': 'src/IMap'},
      classifiers =[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'Intended Audience :: System Administrators',
        'Environment :: Console',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        ])
