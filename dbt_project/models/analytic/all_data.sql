SELECT 
    p.PatientID,
    p.firstname || ' ' || p.lastname AS PatientName,
    p.email AS PatientEmail,
    d.DoctorID,
    d.DoctorName AS DoctorName,
    d.Specialization AS DoctorSpecialization,
    a.AppointmentID,
    a.Date AS AppointmentDate,
    mp.ProcedureID,
    mp.ProcedureName
FROM {{ ref('curated_patients') }} p
JOIN {{ ref('curated_appointments') }} a ON p.PatientID = a.PatientID
JOIN {{ ref('curated_doctors') }} d ON a.DoctorID = d.DoctorID
LEFT JOIN {{ ref('curated_medicalprocedure') }} mp ON a.AppointmentID = mp.AppointmentID
