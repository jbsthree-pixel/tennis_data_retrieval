# tennis_data_retrieval

## CIZR singles retrieval

Run the Streamlit interface:

```powershell
python -m streamlit run src\streamlit_app.py
```

Then click **Retrieve singles data**. The app downloads the CIZR Singles export with the bundled integration token and writes:

- `output\singles.json`
- `output\singles.csv.gz`

The command-line retrieval still works:

```powershell
python src\fetch_singles.py
```
