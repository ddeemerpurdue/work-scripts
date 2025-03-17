import requests
from bs4 import BeautifulSoup
import pandas as pd
import re


institutions = [
    "Abilene Christian University",
    "Air Force Institute of Technology-Graduate School of Engineering & Management",
    "Albert Einstein College of Medicine",
    "Appalachian State University",
    "Arkansas State University",
    "Augusta University",
    "Azusa Pacific University",
    "Ball State University",
    "Boise State University",
    "Bowling Green State University-Main Campus",
    "California State Polytechnic University-Pomona",
    "California State University-Fresno",
    "California State University-Fullerton",
    "California State University-Long Beach",
    "California State University-Los Angeles",
    "California State University-Sacramento",
    "Central Michigan University",
    "Chapman University",
    "Claremont Graduate University",
    "Clark Atlanta University",
    "Clark University",
    "Clarkson University",
    "Cleveland State University",
    "Creighton University",
    "CUNY City College",
    "CUNY Hunter College",
    "Delaware State University",
    "DePaul University",
    "Duquesne University",
    "East Tennessee State University",
    "East Texas A&M",
    "Eastern Michigan University",
    "Eastern Virginia Medical School",
    "Embry-Riddle Aeronautical University-Daytona Beach",
    "Florida Agricultural and Mechanical University",
    "Florida Institute of Technology",
    "Fordham University",
    "Georgia Southern University",
    "Hampton University",
    "Hofstra University",
    "Icahn School of Medicine at Mount Sinai",
    "Idaho State University",
    "Illinois Institute of Technology",
    "Illinois State University",
    "Indiana University of Pennsylvania-Main Campus",
    "Jackson State University",
    "James Madison University",
    "Kean University",
    "Kennesaw State University",
    "Lamar University",
    "Loma Linda University",
    "Long Island University",
    "Louisiana State University Health Sciences Center-New Orleans",
    "Louisiana Tech University",
    "Loyola Marymount University",
    "Marquette University",
    "Marshall University",
    "Medical College of Wisconsin",
    "Mercer University",
    "Miami University-Oxford",
    "Middle Tennessee State University",
    "Montclair State University",
    "Morgan State University",
    "North Carolina A & T State University",
    "Northern Illinois University",
    "Oakland University",
    "Oklahoma State University Center for Health Sciences",
    "Oregon Health & Science University",
    "Pepperdine University",
    "Portland State University",
    "Prairie View A & M University",
    "Rochester Institute of Technology",
    "Rockefeller University",
    "Rowan University",
    "Rush University",
    "Rutgers University-Camden",
    "Rutgers University-Newark",
    "Saint Joseph's University",
    "Sam Houston State University",
    "San Francisco State University",
    "San Jose State University",
    "Seton Hall University",
    "South Carolina State University",
    "South Dakota State University",
    "Southern Connecticut State University",
    "Southern University and A & M College",
    "Stevens Institute of Technology",
    "SUNY College of Environmental Science and Forestry",
    "Tarleton State University",
    "Teachers College at Columbia University",
    "Tennessee State University",
    "Tennessee Technological University",
    "Texas A & M University-Corpus Christi",
    "Texas A & M University-Kingsville",
    "Texas Christian University",
    "Texas Southern University",
    "Texas State University",
    "Texas Tech University Health Sciences Center",
    "Texas Woman's University",
    "The New School",
    "The University of Texas at Tyler",
    "The University of Texas Medical Branch at Galveston",
    "The University of Texas Rio Grande Valley",
    "The University of West Florida",
    "Thomas Jefferson University",
    "University of Akron Main Campus",
    "University of Alabama in Huntsville",
    "University of Alaska Fairbanks",
    "University of Arkansas at Little Rock",
    "University of Arkansas for Medical Sciences",
    "University of Colorado Colorado Springs",
    "University of Massachusetts Chan Medical School",
    "University of Massachusetts-Dartmouth",
    "University of Michigan-Dearborn",
    "University of Missouri-St Louis",
    "University of Nebraska at Omaha",
    "University of New England",
    "University of New Orleans",
    "University of North Carolina at Greensboro",
    "University of North Carolina Wilmington",
    "University of North Florida",
    "University of Northern Colorado",
    "University of Oklahoma-Health Sciences Center",
    "University of Puerto Rico-Mayaguez",
    "University of Puerto Rico-Rio Piedras",
    "University of San Diego",
    "University of San Francisco",
    "University of South Alabama",
    "University of South Dakota",
    "University of Tulsa",
    "Upstate Medical University",
    "Villanova University",
    "Virginia State University",
    "Wake Forest University",
    "West Chester University of Pennsylvania",
    "Western Michigan University",
    "Wichita State University",
    "Wright State University-Main Campus",
    "Yeshiva University"
]


def reformat_institution_name(institution: str) -> tuple[str, str, str]:
    first_try = institution.replace(" ", "-").lower()
    first_try = first_try.replace("&", "and")
    first_try = first_try.replace("'", "")
    second_try = '-'.join([first_try, 'main', 'campus'])
    third_try = '-'.join(['the', first_try])
    fourth_try = re.sub(r"-(?=[^-]*$)", "-at-", first_try)
    fifth_try = re.sub(r"^(.*)-([^-\s]+)-([^-\s]+)$", r"\1-at-\2-\3", first_try)
    return (first_try, second_try, third_try, fourth_try, fifth_try)


def institution_to_url(institution: str) -> str:
    urls = []
    url_start = "https://www.collegefactual.com/colleges"
    url_end = "academic-life/faculty-composition/"
    url_options = reformat_institution_name(institution)
    for url in url_options:
        urls.append('/'.join([url_start, url, url_end]))
    return urls


def clean_the_df(df: pd.DataFrame) -> pd.DataFrame:
    cols = ['full-time faculty', 'tenured faculty', 'tenure-track faculty', 'FT non-tenure faculty', 'part-time faculty', 'grad assistants']
    new_df = pd.DataFrame(columns=cols)

    full_time = df.loc['Total of Those With Faculty Status', 'Full Time']
    tenured = df.loc['Tenured Faculty', 'Full Time']
    tenure_track = df.loc['On Tenure Track', 'Full Time']
    non_tenure = df.loc['Not on Tenure Track', 'Full Time']
    part_time = df.loc['Total of Those With Faculty Status', 'Part Time']
    grad_assistants = df.loc['Graduate Assistants', 'Total']

    new_df.loc[0] = [full_time, tenured, tenure_track, non_tenure, part_time, grad_assistants]
    return new_df


def get_institution_data(institutions: list) -> pd.DataFrame:
    print(f'Found {len(institutions)} institutions')
    missing_insitutions = []
    all_dfs = []

    for cnt, institution in enumerate(institutions):
        print(f"{cnt + 1}/{len(institutions)} Fetching data for {institution}")
        
        urls = institution_to_url(institution)

        good_response = False
        for cnt, url in enumerate(urls):
            response = requests.get(url)
            if response.status_code == 200:
                good_response = True
                break
        
        if not good_response:
            print(f"\n[FAILED] #{cnt} to fetch the webpage for {institution}\nTried for following: {urls}\n\n")
            missing_insitutions.append(institution)
            continue

        # Parse the HTML content
        soup = BeautifulSoup(response.text, "html.parser")

        # Find the table element
        table = soup.find("table", {"class": "table table-striped"})
        html_str = str(table)
        import io
        dfs = pd.read_html(io.StringIO(html_str))
        df = dfs[0]
        df.set_index(df.columns[0], inplace=True)
        clean_df = clean_the_df(df)
        clean_df['university'] = institution

        all_dfs.append(clean_df)

    master_df = pd.concat(all_dfs)
    return master_df, missing_insitutions


dfs, missing_institutions = get_institution_data(institutions)
print(f'{len(missing_institutions)} missing institutions:\n{missing_institutions}')

dfs.to_csv("faculty_composition_r2-clean.csv")
