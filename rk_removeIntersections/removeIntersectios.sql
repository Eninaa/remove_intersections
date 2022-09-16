CREATE OR REPLACE FUNCTION rk_removeIntersections(geom1str text, geom2str text, bufferSize double precision, iterationsCount bigint, simplifyPrecision double precision, loopPrecision double precision, sliverArea double precision) 
RETURNS TABLE (g1 text, g2 text) AS $$
	DECLARE
		geom1 geometry;
     	geom2 geometry;
		
		temp1 geometry;
     	temp2 geometry;
		
     	geomx geometry;
     	geom1Buffer geometry;
     	geom2Buffer geometry;
     	area double precision;
		area2 double precision;
     	intersec geometry;
		count int;
		type text;
		g_temp1 geometry[];
		g_temp2 geometry[];

		i int;
	BEGIN
	geom1 := ST_GeomFromGeoJSON(geom1str); 
	geom2 := ST_GeomFromGeoJSON(geom2str);
	count := 0;
	i := 1;
	
	BEGIN
	geomx := ST_Intersection(geom1, geom2);
	area := ST_Area(geomx);
	area2 := area;
	
	IF ST_Intersects(geom1, geom2) THEN
	/*при вычитании geomx получаются полигоны с дырами*/
		temp1 := geom1;
		temp2 := geom2;
  		geom1 := ST_Difference(geom1, temp2);
  		geom2 := ST_Difference(geom2, temp1);
  	END IF;
 
	type := ST_GeometryType(geom1);
	IF (type = 'ST_MultiPolygon') THEN
		WHILE i <= ST_NumGeometries(geom1) LOOP
			IF (ST_Area(ST_GeometryN(geom1, i)) > sliverArea) THEN
				g_temp1 := array_append(g_temp1, ST_GeometryN(geom1, i));
			END IF;
			i := i + 1;
		END LOOP;
	IF (cardinality(g_temp1) = 1) THEN
		geom1 := g_temp1[1];
	ELSE 
		geom1 := ST_Collect(g_temp1);
	END IF;
		i := 1;
	END IF;
		
	type := ST_GeometryType(geom2);
		IF (type = 'ST_MultiPolygon') THEN
			WHILE i <= ST_NumGeometries(geom2) LOOP
		IF (ST_Area(ST_GeometryN(geom2, i)) > sliverArea) THEN
			g_temp2 := array_append(g_temp2, ST_GeometryN(geom2, i));
		END IF;
		i := i + 1;
			END LOOP;
		IF (cardinality(g_temp2) = 1) THEN
			geom2 := g_temp2[1];
		ELSE 
			geom2 := ST_Collect(g_temp2);
		END IF;
		i := 1;
		END IF;

     WHILE area > loopPrecision LOOP

        geom1Buffer := ST_Buffer(geom1, bufferSize,'join=mitre');
        intersec := ST_Intersection(geomx, geom1Buffer);
		geom1 := ST_Union(geom1, intersec);
		geom1 := ST_Intersection(ST_Buffer(geom1, bufferSize,'join=mitre'), geom1);
        geomx := ST_Difference(geomx, intersec); 

        geom2Buffer := ST_Buffer(geom2, bufferSize,'join=mitre');
        intersec := ST_Intersection(geomx, geom2Buffer);
		geom2 := ST_Union(geom2, intersec);
		geom2 := ST_Intersection(ST_Buffer(geom2, bufferSize,'join=mitre'), geom2);


        geomx := ST_Difference(geomx, intersec);
        area = ST_Area(geomx);
		IF (abs(area - area2) < 1.0e-10 OR count = iterationsCount) THEN
			geom1 := ST_CollectionExtract(geom1, 3);
			geom1 := ST_Intersection(ST_Buffer(geom1, bufferSize,'join=mitre'), geom1);

  			geom2 := ST_CollectionExtract(geom2, 3);
			geom2 := ST_Intersection(ST_Buffer(geom2, bufferSize,'join=mitre'), geom2);

			RETURN QUERY SELECT ST_AsGeoJson(ST_Simplifyvw(geom1,simplifyPrecision)), ST_AsGeoJson(ST_Simplifyvw(geom2 ,simplifyPrecision));
			RETURN;
		END IF;
		area2 := area;
		count := count + 1;
		geom1 := ST_CollectionExtract(geom1, 3);
  		geom2 := ST_CollectionExtract(geom2, 3);
		
	END LOOP;
	EXCEPTION WHEN OTHERS THEN 
		geom1 := ST_CollectionExtract(geom1, 3);
  		geom2 := ST_CollectionExtract(geom2, 3);
	end;
	RETURN QUERY SELECT ST_AsGeoJson(ST_Simplifyvw(geom1,simplifyPrecision)), ST_AsGeoJson(ST_Simplifyvw(geom2 ,simplifyPrecision));
    END;
$$ LANGUAGE plpgsql;