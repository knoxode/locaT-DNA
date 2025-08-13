import streamlit as st

def generate_jbrowse_session():
    """
    Generate a JBrowse 2 session URL with a test data configuration.
    This is a placeholder function that can be replaced with actual session generation logic.
    """
    return "https://jbrowse.org/code/jb2/main/?config=test_data%2Fconfig.json"

def jbrowse_viewer(jbrowse_url="https://jbrowse.org/code/jb2/main/?config=test_data%2Fconfig.json"):
    """
    Embed a JBrowse 2 session in Streamlit using an iframe.
    :param jbrowse_url: URL to the JBrowse instance or config.
    """
    st.header("Your Alignment in JBrowse")
    st.components.v1.iframe(jbrowse_url, height=600, scrolling=True)

# Example usage in your Streamlit app:
# jbrowse_viewer()  # Uses the public JBrowse demo
# jbrowse_viewer("http://localhost:3000")  # For your local JBrowse instance