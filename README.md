# sparkRepoProject

The pySpark codes in the above links to the following data, you will need to download them yourself from https://data.sfgov.org<br>

Here is the list of the .py file - the data source:<br><br>
businessPerZipCodeInSanFrancisco.py: <br>
https://data.sfgov.org/Economy-and-Community/Registered-Business-Locations-San-Francisco/g8m3-pdis

whoIsTheFoodTruckKing.py<br>
https://data.sfgov.org/Economy-and-Community/Mobile-Food-Facility-Permit/rqzj-sfat

LookUpTheseDetails.py <br>
The dataset uses u.data and u.item file for lookup puposes. There are some comments in there to help with spark learning. I will talk about that in my course as well.


Broadcast_for_school.py <br>
https://data.sfgov.org/Economy-and-Community/Schools/tpp3-epx2 and <br>
https://data.sfgov.org/Economy-and-Community/Registered-Business-Locations-San-Francisco/g8m3-pdis`

sparkSQL.py<br>
Using FakeFriends.csv The data is not mine, but i will be using this as a base model to analyze some open data from San Francisco. <br>

sparkSQL_subquery<br>
using u.data and u.item two subdatasets and with df.join(df1, how, on='key') for  left join. The spark dataframe doesnt work well with datetime, so recommend now to change timestamp with pd.to_datetime().<br>

airtraffic_opensf.py <br>
airtraffic_opensf_sql_version_included.py <br>
airtraffic_opensf_sql_version_Pandas_included.py<br>
https://data.sfgov.org/Transportation/Air-Traffic-Passenger-Statistics/rkru-6vcg

Air_traffic_landings_stats.py<br>
https://data.sfgov.org/Transportation/Air-Traffic-Landings-Statistics/fpux-q53t

SparkStreamingTwitter.scala <br>
Need Aouth from twitter, set up developer account first then use Utilities.scala and SparkStreaming Twitter.scala together to save twits.

GlobeNdMail.py will be the last of the projects I will commit to this Repo. It includes my work for 15 hours which produced a spark pipeline to get desired output based on some parquet files in the gzip format. 
