# tennis_data_retrieval

## CIZR singles retrieval

Run the Streamlit interface:

```powershell
python -m streamlit run src\streamlit_app.py
```

Then click **Retrieve singles data**. The app downloads the CIZR Singles export with the bundled integration token and provides a browser download for:

- `output\singles.csv.gz`
- reviewable one-row-per-match results inside the app
- selected match-name rename proposals saved to `output\proposed_match_name_updates.csv`
- dry-run or execute reuploads of selected match-name changes back to CIZR, with audit CSVs in `output\`

The command-line retrieval still works:

```powershell
python src\fetch_singles.py
```

Match-name reuploads can also be run from the command line after preparing `output\proposed_match_name_updates.csv`:

```powershell
python src\main.py --apply-match-name-updates --all
python src\main.py --apply-match-name-updates --all --execute
```
