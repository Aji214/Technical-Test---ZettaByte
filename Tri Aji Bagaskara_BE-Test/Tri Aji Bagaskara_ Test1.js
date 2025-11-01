function result(sessions) {
  const groupedSessions = [];

  for (const s of sessions) {
    let session = groupedSessions.find(item => item.session_id === s.session_id);
    if (!session) {
      session = { session_id: s.session_id, time: s.time, classes: [] };
      groupedSessions.push(session);
    }
    let cls = session.classes.find(c => c.class_id === s.class.class_id);
    if (!cls) {
      cls = { class_id: s.class.class_id, name: s.class.name, students: [] };
      session.classes.push(cls);
    }
    cls.students.push({
      student_id: s.student.student_id,
      name: s.student.name
    });
  }

  return groupedSessions;
}

console.log(JSON.stringify(result(sessions), null, 2));
