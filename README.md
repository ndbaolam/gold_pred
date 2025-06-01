```sh
python3 -m venv venv
source venv/bin/activate #Ubuntu
.\venv\Scripts\Activate.ps1 #Window
pip install -r requirements.txt
dvc pull
```

```sh
dvc add data
dvc push
git add .
git commit -m "Some message"
```