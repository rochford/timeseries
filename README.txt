TimeSeries is a time series database written in Go. A timeseries is a collection
of Observations at points in time. Observations can be about different things or
the same thing. The timeSeries stores observations for the same thing in its own
bucket. The bucket is a set of sequential observations over a given period of
time such as a minute, an hour or a day.

When an observation is added to the TimeSeries it is added with a timestamp to
record when the observation took place.

Observations can have multiple Key-Value pairs called Tags associated with the
observation. The first key-value pair identifies the thing that the observation
is about, but the others are client defined. For example, an obseration that a
particular stock had such a selling price would include 2 tags.  The first tag
would identify the stock and the second one the selling price. Other tags could
be added for example to record price sensitive news about the stock.
