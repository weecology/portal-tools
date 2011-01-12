USE Portal;
SELECT Rodents.*, Rodents.period, Rodents.stake, SPECIES.Rodent
FROM SPECIES INNER JOIN Rodents ON SPECIES.`New Code` = Rodents.sp
WHERE (((Rodents.period) > 6) AND 
    ((Rodents.stake) > 10 AND (Rodents.stake) < 78) AND ((SPECIES.Rodent)=1));
