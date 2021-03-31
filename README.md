# README

## Prepare
Create a correct environment e.g. using `conda`. We need quite specific versions, e.g. python 3.7 and some other specific versions of some packages. So save yourself some pain and do it the quick way ;)
```
conda create --name myenv --file package-list.txt
```
Make shell script executable variance
```
chmod +x update_data.sh
```

## Run analysis
```
./update_data.sh
```
## Download Historical Data
python download_covid_hist_data.py
python download_pm25_hist_data.py
python concatenate_all_files_in_dir.py