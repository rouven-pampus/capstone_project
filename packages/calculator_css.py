# Custom CSS to change slider color
custom_css = """
<style>
/* Full slider track background */
div.stSlider > div[data-baseweb="slider"] > div:nth-child(1) > div:nth-child(1) {
    background: rgba(38, 144, 155, 0.3) !important;
}
/* Slider thumb */
div[role="slider"] {
    background: rgb(38, 144, 155) !important;
    border-color: rgb(38, 144, 155) !important;
}
div[role="slider"]:focus {
    background: rgb(38, 144, 155) !important;
    border-color: rgb(38, 144, 155) !important;
}
div[role="slider"]:hover {
    background: rgb(38, 144, 155) !important;
    border-color: rgb(38, 144, 155) !important;
    box-shadow: 0 0 0 0.3rem rgba(38, 144, 155, 0.25) !important;
}
/* Number above the slider */
div.StyledThumbValue {
    color: rgb(38, 144, 155) !important;
}
/* Min and max values */
div[data-testid="stTickBarMin"],
div[data-testid="stTickBarMax"] {
    color: rgb(38, 144, 155) !important;
}
/* Selected radio button background */
div[role="radiogroup"] > label > div.st-bg {
    background: rgb(38, 144, 155) !important;
    border-color: rgb(38, 144, 155) !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
}
/* Change color for selection box when open */
div[data-baseweb="select"] > div:hover,
div[data-baseweb="select"] > div:focus {
    border-color: rgb(38, 144, 155) !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
}
button:hover {
    color: rgb(38, 144, 155) !important;
    border-color: rgb(38, 144, 155) !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
    background-color: transparent !important;
}
button:focus {
    color: rgb(38, 144, 155) !important;
    border-color: rgb(38, 144, 155) !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
    background-color: transparent !important;
}
div[data-testid="stNumberInput"] input[type="number"] button[class="step-down"]:hover,
div[data-testid="stNumberInput"] input[type="number"] button[class="step-up"]:hover {
    background-color: rgb(38,144,155) !important;
    color: rgb(38,144,155) !important;
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
}
/* Change border color on hover */
div[data-testid="stNumberInput"] div:hover {
    border-color: rgb(38, 144, 155); /* Hover border color */
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
}
/* Change border color on focus (click) */
div[data-testid="stNumberInput"] input[type="number"]:focus, 
div[data-testid="stNumberInput"] div:focus-within {
    border-color: rgb(38,144,155); /* Click border color */
    box-shadow: 0 0 0 0.2rem rgba(38, 144, 155, 0.25) !important;
}
</style>
"""