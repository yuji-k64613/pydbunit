docker run -it --rm --name python -v $(pwd):/work python:3 /bin/bash
cd /work
python3 -m unittest sample.py
