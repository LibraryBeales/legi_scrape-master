import plotly.express as px

# Sample data
df = px.data.gapminder().query("year == 2007")

# Create a scatter plot
fig = px.scatter(
    df,
    x="gdpPercap",
    y="lifeExp",
    size="pop",
    color="continent",
    log_x=True,
    title="Life Expectancy vs GDP per Capita (2007)"
)

# Save figure using Kaleido
fig.write_image("gapminder_plot.png", engine="kaleido")
