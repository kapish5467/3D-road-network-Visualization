

start_part = open("/opt/hadoop/home/hadoop2/CS226_code/Start.kml").read()

end_part = """</coordinates>
 </LineString> </Placemark>"""

final_end = """
 </Document> </kml>"""

start_linestring = """ <Placemark>
 <name>Absolute Extruded</name>
 <description>Transparent green wall with yellow outlines</description>
 <styleUrl>#yellowLineGreenPoly</styleUrl>
 <LineString>
 <extrude>0</extrude>
 <tessellate>1</tessellate>
 <altitudeMode>absolute</altitudeMode>
 <coordinates> """
count= 1

write = start_part



for i in range(100):
    f = open("/opt/hadoop/home/hadoop2/data/output_kml/"+ str(i) + ".kml","w+")

    if i <10:
        text = open("/opt/hadoop/home/hadoop2/data/output/dem1/part-0000" + str(i)).read()
    else:
        text = open("/opt/hadoop/home/hadoop2/data/output/dem1/part-000" + str(i)).read()
    text_list = text.split("\n")
    text_list = [text.split("\t")[1:] for text in text_list]

    for road in text_list:

        road = "\n".join(road)
        write += start_linestring+ road  + end_part


    f.write(write+final_end)
    f.close()

