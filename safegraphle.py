import streamlit as st
import pandas as pd
from datetime import datetime
import random
import snowflake.connector
import plotly.express as px

st.markdown(
    """ 
    # SAFEGRAPHLE 
    Guess the SAFEGRAPHLE in six tries. 
    
    The SAFEGRAPHLE is a SafeGraph brand with the following characteristics:
    * 250 US POIs or more
    * POIs in between 3 and 25 states, and 
    * `naics_code` starts with `7225` (Restaurants and Other Eating Places) or `4451` (Grocery Stores).

    **Here is a map of all US locations for the specified SAFEGRAPHLE (Updates every weekday):**
    """
    )

new_game_button = st.button("OR TRY A RANDOM SAFEGRAPHLE")
randomize_answer = False
if new_game_button:
    st.session_state.pop("guesses",None)
    st.session_state.pop("answer_idx",None)
    randomize_answer = True


# Functions for pulling data from Snowflake
# Initialize connection.
# Uses st.cache to only run once.
@st.cache(allow_output_mutation=True, hash_funcs={"_thread.RLock": lambda _: None,"builtins.weakref": lambda _: None,})
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])

conn = init_connection()

# Populate answer list
@st.cache(hash_funcs={"_thread.lock": lambda _: None})
def get_safegraph_brands():
    safegraphle_query = f'''
    WITH brand_summary AS (
    SELECT 
        brands,
        naics_code, top_category, sub_category,
        count(*) as num_pois,
        count(distinct region) as num_states,
        arrayagg(distinct region) within group (order by region asc)  as states,
        safegraph_brand_ids
    FROM "SG_CORE_PLACES_US"."PUBLIC"."CORE_POI"
    WHERE safegraph_brand_ids IS NOT NULL
    GROUP BY brands, safegraph_brand_ids, naics_code, top_category, sub_category
    ORDER BY num_pois desc
    )
    SELECT * FROM brand_summary
    WHERE NUM_STATES BETWEEN 3 and 25
    AND NUM_POIS > 250
    AND (startswith(naics_code,'7225') OR startswith(naics_code,'4451'))
    ORDER BY naics_code DESC, num_pois DESC
    '''
    return pd.read_sql(safegraphle_query, conn)

# Get answer list
answers = get_safegraph_brands()

# Order answers by index of query

# Initial code to get order:
# first_answers = [18, 42, 0, 51, 6]
# remaining_answers = [idx for idx in list(answers.index) if idx not in first_answers]
# random.shuffle(remaining_answers)
# answer_order = [first_answers + remaining_answers]

# Manually entered order adusting for weekends
answer_order = \
    [18, 42, 0, 51, 6, 6, 6,
    58, 64, 43, 25, 12, 12, 12, 
    11, 41, 29, 5, 23, 23, 23, 
    8, 2, 19, 38, 48, 48, 48,
    10, 60, 54, 47, 3, 3, 3, 
    39, 9, 45, 31, 40, 40, 40, 
    36, 30, 63, 57, 37, 37, 37, 
    1, 21, 28, 35, 16, 16, 16, 
    32, 34, 15, 26, 52, 52, 52, 
    27, 14, 33, 24, 13, 13, 13,
    7, 46, 4, 53, 55, 55, 55, 
    49, 61, 50, 56, 22, 22, 22, 
    62, 17, 20, 59, 44, 44, 44]

# Get specific answer for the day
days_since_mar28 = (datetime.now() - datetime(2022,3,28)).days

if "answer_idx" not in st.session_state:
    if randomize_answer:
        st.session_state["answer_idx"] = round(random.uniform(0,1)*len(answers))        
    else:
        st.session_state["answer_idx"] = answer_order[days_since_mar28]
answer = answers.loc[[st.session_state["answer_idx"]]]

# Query for brand locations for answer.
@st.cache(hash_funcs={"_thread.lock": lambda _: None})
def get_safegraphle_pois(answer_brand_id):
    poi_query = f'''
    SELECT *
    FROM SG_CORE_PLACES_US.PUBLIC.CORE_POI
    WHERE safegraph_brand_ids LIKE '%{answer_brand_id}%'
    '''
    return pd.read_sql(poi_query, conn)

answer_pois = (
    get_safegraphle_pois(answer["SAFEGRAPH_BRAND_IDS"].values[0])[["LATITUDE","LONGITUDE"]]
        .rename({"LATITUDE":"lat","LONGITUDE":"lon"},axis='columns')
)

# Show map
st.text(f"SAFEGRAPHLE #{st.session_state['answer_idx']}")
st.map(answer_pois,zoom=3)
# st.write(answer)

# Previous guesses
if "guesses" not in st.session_state:
    st.session_state["guesses"] = []
    st.session_state["guess_naics_codes"] = []
    st.session_state["guess_states"] = []
    st.session_state["guess_num_pois"] = []
    st.session_state["num_guesses"] = 0
    st.session_state["summary"] = f"SAFEGRAPHLE #{st.session_state['answer_idx']}\n"

class Brand:
    def __init__(self, df_row):
        self.naics = str(df_row["NAICS_CODE"].values[0])
        self.pois = df_row["NUM_POIS"].values[0]
        self.states = eval(df_row["STATES"].values[0])
        self.num_states = df_row["NUM_STATES"].values[0]

class Guess:
    def __init__(self, guess_df, answer_df):
        self.a = Brand(answer_df)
        self.g = Brand(guess_df)
        self.naics = self.g.naics
        self.pois = self.g.pois
        self.states = self.g.states

    def check_naics(self):
        g_code, a_code = self.g.naics, self.a.naics
        if g_code == a_code:
            return "🟩"
        elif g_code[0:4] == a_code[0:4]:
            return "🟨"
        else:
            return "⬜"

    def check_num_pois(self):
        diff = self.g.pois - self.a.pois
        if diff == 0:
            return "🟩"
        elif diff > 0:
            return "⬇️"
        else:
            return "⬆️"

    def check_states(self):
        same_states = set(self.g.states).intersection(set(self.a.states))
        pct_correct_states = len(same_states) / self.a.num_states
        if pct_correct_states == 1:
            return "🟩"
        elif pct_correct_states > 0.5:
            return "🟨"
        else:
            return "⬜"

# Guess stuff
guess_box = st.selectbox('Guess a brand (Guess is not attempted until you hit GUESS):', answers,format_func=(lambda x: x.upper()))

click_button = st.button("GUESS", disabled = st.session_state["num_guesses"]>=6)
if click_button:
    guess_df = answers.loc[answers["BRANDS"]==guess_box]
    latest_guess = Guess(guess_df, answer)
    st.session_state["num_guesses"]+=1

    # st.write(guess_df)            
    st.session_state["guesses"].append(guess_box)
    st.session_state["guess_naics_codes"].append(f"{latest_guess.naics} {latest_guess.check_naics()}")
    st.session_state["guess_states"].append(f"{str(latest_guess.states)} {latest_guess.check_states()}")
    st.session_state["guess_num_pois"].append(f"{str(latest_guess.pois)} {latest_guess.check_num_pois()}")
    st.session_state["summary"] += f"{latest_guess.check_naics()} {latest_guess.check_states()} {latest_guess.check_num_pois()}\n"


    if guess_box == answer["BRANDS"].values[0]:
        st.text(f"🎉 YOU WIN! {st.session_state['num_guesses']}/6")
        st.text(st.session_state["summary"])
    elif st.session_state["num_guesses"]>=6:
        st.text("🙃 BETTER LUCK NEXT TIME! X/6")
        st.text(st.session_state["summary"])
        st.text(f"THE ANSWER WAS: {answer['BRANDS'].values[0]} (Don't share this part!)")
    
# Display guesses
guesses_dict = {"NAICS_CODE": st.session_state["guess_naics_codes"],
                "STATES":     st.session_state["guess_states"],
                "NUM_POIS":   st.session_state["guess_num_pois"]}
st.markdown(
    """
    ---
    **Your guesses**
    """
    )
display_guesses = pd.DataFrame(guesses_dict,index=st.session_state["guesses"])
st.table(display_guesses)

st.markdown(
    """
    - NAICS_CODE: 🟩 = Correct; 🟨 = Correct first 4 digits; ⬜ = Way off
    - STATES: 🟩 = Captures 100% of correct states; 🟨 = Captures >50% correct states; ⬜ = Way off
    - NUM_POIS: 🟩 = Correct; ⬇️ = Too high; ⬆️ = Too low
    """
    )