async function UpdateCandidate(
  parent,
  {
    _id,
    candidate_input,
    lang,
    new_desired_program,
    is_from_admission_form,
    is_prevent_resend_notif,
    is_save_identity_student,
    is_minor_student,
  },
  context
) {
  let tokenUser = String(context.req.headers.authorization).replace('Bearer ', '');
  tokenUser = tokenUser.replace(/"/g, '');

  let userId;

  if (tokenUser && tokenUser !== 'undefined') {
    userId = common.getUserId(tokenUser);
  }


 // *************** Find candidate first before update
const candidateBeforeUpdate = await CandidateModel.findById(_id)
  .select('iban payment_supports parents')
  .lean();

// *************** Normalize school & campus fields
['school', 'campus'].forEach((field) => {
  if (candidate_input[field]) {
    candidate_input[field] = String(candidate_input[field]).toUpperCase();
  }
});

// *************** Determine sex based on civility
if (candidate_input.civility) {
  const civilityMap = { MR: 'M', MRS: 'F', MS: 'F', neutral: 'N' };
  candidate_input.sex = civilityMap[candidate_input.civility] || 'N';
}

// *************** Validate & store IBAN/BIC history for parents
if (Array.isArray(candidate_input.parents) && candidate_input.parents.length > 0) {
  await Promise.all(
    candidate_input.parents.map(async (parent) => {
      if (parent.iban && parent.bic && parent.account_holder_name) {
        const ibanHistory = await IbanHistoryModel.create({
          candidate_id: _id,
          iban: parent.iban,
          bic: parent.bic,
          account_holder_name: parent.account_holder_name,
          financial_support_first_name: parent.name || '',
          financial_support_last_name: parent.family_name || '',
        });

        try {
          await CandidateUtility.validateIbanBicCandidate(parent.iban, parent.bic);
          await IbanHistoryModel.updateOne(
            { _id: ibanHistory._id },
            { $set: { message: 'success' } }
          );
        } catch (error) {
          await IbanHistoryModel.updateOne(
            { _id: ibanHistory._id },
            { $set: { message: error.message || String(error) } }
          );

          throw new ApolloError(error.message || 'IBAN validation failed');
        }
      }
    })
  );
}

// *************** Ensure tag_ids is always an array
if (!Array.isArray(candidate_input.tag_ids)) {
  candidate_input.tag_ids = [];
}
// *************** Validate IBAN & BIC of candidate
if (
  candidate_input.iban &&
  candidate_input.bic &&
  candidate_input.account_holder_name
) {
  const ibanHistory = await IbanHistoryModel.create({
    candidate_id: _id,
    iban: candidate_input.iban,
    bic: candidate_input.bic,
    account_holder_name: candidate_input.account_holder_name,
  });

  try {
    // *************** Validate candidate's IBAN input
    await CandidateUtility.validateIbanBicCandidate(
      candidate_input.iban,
      candidate_input.bic
    );

    // *************** Update IBAN history with success message
    await IbanHistoryModel.updateOne(
      { _id: ibanHistory._id },
      { $set: { message: 'success' } }
    );
  } catch (error) {
    // *************** Update IBAN history with error message
    await IbanHistoryModel.updateOne(
      { _id: ibanHistory._id },
      { $set: { message: error.message || String(error) } }
    );

    throw new ApolloError(error.message || 'IBAN validation failed');
  }
}

// *************** Validate IBAN & BIC of payment supports
if (
  Array.isArray(candidate_input.payment_supports) &&
  candidate_input.payment_supports.length > 0
) {
  await Promise.all(
    candidate_input.payment_supports.map(async (support) => {
      if (support.iban && support.bic && support.account_holder_name) {
        const ibanHistory = await IbanHistoryModel.create({
          candidate_id: _id,
          iban: support.iban,
          bic: support.bic,
          account_holder_name: support.account_holder_name,
          financial_support_first_name: support.name || '',
          financial_support_last_name: support.family_name || '',
        });

        try {
          // *************** Validate payment support's IBAN input
          await CandidateUtility.validateIbanBicCandidate(
            support.iban,
            support.bic
          );

          // *************** Update IBAN history with success message
          await IbanHistoryModel.updateOne(
            { _id: ibanHistory._id },
            { $set: { message: 'success' } }
          );
        } catch (error) {
          // *************** Update IBAN history with error message
          await IbanHistoryModel.updateOne(
            { _id: ibanHistory._id },
            { $set: { message: error.message || String(error) } }
          );

          throw new ApolloError(error.message || 'IBAN validation failed');
        }
      }
    })
  );
}

// *************** If candidate data before update exists
if (candidateBeforeUpdate) {
  // *************** Check if IBAN has changed
  const oldIban = candidateBeforeUpdate.iban ? String(candidateBeforeUpdate.iban).trim() : '';
  const newIban = candidate_input.iban ? String(candidate_input.iban).trim() : '';

  if (oldIban && oldIban !== newIban) {
    // *************** Create IBAN history update record
    await IbanHistoryUpdateModel.create({
      candidate_id: _id,
      iban: newIban || null,
      iban_before_update: oldIban || null,
      user_who_update_id: userId,
    });
  }
}

// *************** If IBAN in payment supports is different from input, create IBAN history update
if (
  Array.isArray(candidateBeforeUpdate.payment_supports) &&
  candidateBeforeUpdate.payment_supports.length > 0 &&
  Array.isArray(candidate_input.payment_supports) &&
  candidate_input.payment_supports.length > 0
) {
  await Promise.all(
    candidateBeforeUpdate.payment_supports.map(async (oldSupport) => {
      const matchedSupport = candidate_input.payment_supports.find(
        (newSupport) =>
          String(oldSupport._id || '') === String(newSupport._id || '') &&
          String(oldSupport.iban || '').trim() !== String(newSupport.iban || '').trim()
      );

      if (matchedSupport) {
        // *************** Create IBAN history update record for changed payment support
        await IbanHistoryUpdateModel.create({
          candidate_id: _id,
          iban: matchedSupport.iban ? String(matchedSupport.iban).trim() : null,
          iban_before_update: oldSupport.iban ? String(oldSupport.iban).trim() : null,
          user_who_update_id: userId,
          financial_support_first_name: matchedSupport.name || '',
          financial_support_last_name: matchedSupport.family_name || '',
        });
      }
    })
  );
}
// *************** If IBAN has changed, create IBAN history update
if (paymentSupportIbanData) {
  const oldIban = paymentSupportBeforeUpdate.iban
    ? String(paymentSupportBeforeUpdate.iban).trim()
    : '';
  const newIban = paymentSupportIbanData.iban
    ? String(paymentSupportIbanData.iban).trim()
    : '';

  if (oldIban !== newIban) {
    await IbanHistoryUpdateModel.create({
      candidate_id: _id,
      iban: newIban || null,
      iban_before_update: oldIban || null,
      user_who_update_id: userId,
      financial_support_first_name: paymentSupportIbanData.name || '',
      financial_support_last_name: paymentSupportIbanData.family_name || '',
    });
  }
}

      // *************** If IBAN in parents is deleted or changed from input, create IBAN history update
if (
  Array.isArray(candidateBeforeUpdate.parents) &&
  candidateBeforeUpdate.parents.length > 0 &&
  Array.isArray(candidate_input.parents) &&
  candidate_input.parents.length > 0
) {
  await Promise.all(
    candidateBeforeUpdate.parents.map(async (oldParent) => {
      const matchedParent = candidate_input.parents.find(
        (newParent) =>
          String(oldParent._id || '') === String(newParent._id || '') &&
          String(oldParent.iban || '').trim() !== String(newParent.iban || '').trim()
      );

      if (matchedParent) {
        const oldIban = oldParent.iban ? String(oldParent.iban).trim() : '';
        const newIban = matchedParent.iban ? String(matchedParent.iban).trim() : '';

        // Jika IBAN lama ada dan IBAN baru kosong (berarti dihapus), catat history
        if (oldIban && !newIban) {
          await IbanHistoryUpdateModel.create({
            candidate_id: _id,
            iban: null,
            iban_before_update: oldIban,
            user_who_update_id: userId,
            financial_support_first_name: matchedParent.name || '',
            financial_support_last_name: matchedParent.family_name || '',
          });
        }

        // Jika IBAN lama dan baru berbeda (update)
        if (oldIban && newIban && oldIban !== newIban) {
          await IbanHistoryUpdateModel.create({
            candidate_id: _id,
            iban: newIban,
            iban_before_update: oldIban,
            user_who_update_id: userId,
            financial_support_first_name: matchedParent.name || '',
            financial_support_last_name: matchedParent.family_name || '',
          });
        }
      }
    })
  );
}

// *************** If parent IBAN has changed, create IBAN history update
if (parentIbanData) {
  const oldIban = parentBeforeUpdate.iban
    ? String(parentBeforeUpdate.iban).trim()
    : '';
  const newIban = parentIbanData.iban
    ? String(parentIbanData.iban).trim()
    : '';

  // *************** Only log if IBAN is truly different
  if (oldIban !== newIban) {
    await IbanHistoryUpdateModel.create({
      candidate_id: _id,
      iban: newIban || null,
      iban_before_update: oldIban || null,
      user_who_update_id: userId,
      financial_support_first_name: parentIbanData.name || '',
      financial_support_last_name: parentIbanData.family_name || '',
    });
  }
}

      const nowTime = moment.utc();
      const oldCandidate = await CandidateModel.findById(_id);

// *************** Ensure legal_representative has unique_id
if (candidate_input.legal_representative) {
  const hasOldUniqueId =
    oldCandidate?.legal_representative?.unique_id || null;

  // Jika legal representative belum punya unique_id, ambil dari data lama, kalau tidak ada buat baru
  if (!candidate_input.legal_representative.unique_id) {
    candidate_input.legal_representative.unique_id =
      hasOldUniqueId || common.create_UUID();
  }
}

// *************** If candidate has no civility, use parental_link to assign civility
if (
  candidate_input.legal_representative &&
  !candidate_input.legal_representative.civility &&
  candidate_input.legal_representative.parental_link
) {
  const parentalLink = String(candidate_input.legal_representative.parental_link).toLowerCase().trim();
  const maleRelations = ['father', 'grandfather', 'uncle'];

  if (parentalLink === 'other') {
    candidate_input.legal_representative.civility = '';
  } else if (maleRelations.includes(parentalLink)) {
    candidate_input.legal_representative.civility = 'MR';
  } else {
    candidate_input.legal_representative.civility = 'MRS';
  }
}
// *************** Make legal representative's last name uppercase
if (candidate_input.legal_representative?.last_name) {
  candidate_input.legal_representative.last_name = String(
    candidate_input.legal_representative.last_name
  )
    .trim()
    .toUpperCase();
}

// *************** Failsafe: if candidate finance not set up yet on form filling
if (
  !candidate_input.finance &&
  !oldCandidate.finance &&
  oldCandidate?.selected_payment_plan?.payment_mode_id
) {
  const hasPaymentSupports =
    (Array.isArray(candidate_input.payment_supports) &&
      candidate_input.payment_supports.length > 0) ||
    (Array.isArray(oldCandidate.payment_supports) &&
      oldCandidate.payment_supports.length > 0);

  candidate_input.finance = hasPaymentSupports ? 'family' : 'my_self';
}

// *************** Clone old selected payment plan data safely
let oldSelectedPaymentPlanData = oldCandidate?.selected_payment_plan
  ? JSON.parse(JSON.stringify(oldCandidate.selected_payment_plan))
  : {};

if (Array.isArray(oldSelectedPaymentPlanData.payment_date)) {
  oldSelectedPaymentPlanData.payment_date = oldSelectedPaymentPlanData.payment_date.map((term) => {
    const { _id, ...rest } = term;
    return rest;
  });
} else {
  oldSelectedPaymentPlanData.payment_date = [];
}

  // *************** If candidate is registered and email has changed, trigger email update logic
if (
  oldCandidate?.user_id &&
  oldCandidate?.candidate_admission_status === 'registered'
) {
  const oldEmail = oldCandidate.email ? String(oldCandidate.email).trim().toLowerCase() : '';
  const newEmail = candidate_input.email ? String(candidate_input.email).trim().toLowerCase() : '';

  if (newEmail && oldEmail !== newEmail) {
    // *************** Candidate's email changed after registration
    // (Add your email update logic here, e.g. sync to user model or send verification)
  }
}

// *************** If candidate email changed, update user email and clear recovery code
if (oldCandidate?.user_id && candidate_input?.email) {
  const result = await UserModel.updateOne(
    { _id: oldCandidate.user_id },
    {
      $set: {
        email: candidate_input.email.trim().toLowerCase(),
        recovery_code: '',
      },
    }
  );

  // *************** Optional: log update result for debugging
  if (result.modifiedCount > 0) {
    console.log(
      `✅ User email updated and recovery code cleared for user_id: ${oldCandidate.user_id}`
    );
  } else {
    console.warn(
      `⚠️ No user record updated for user_id: ${oldCandidate.user_id}`
    );
  }
}

// *************** Update candidate email so notifications use the new email
if (oldCandidate?._id && candidate_input?.email) {
  const normalizedEmail = String(candidate_input.email).trim().toLowerCase();

  const updateResult = await CandidateModel.updateOne(
    { _id: oldCandidate._id },
    { $set: { email: normalizedEmail } }
  );

  // *************** Optional: log result for debugging or auditing
  if (updateResult.modifiedCount > 0) {
    console.log(
      `✅ Candidate email updated successfully for candidate_id: ${oldCandidate._id}`
    );
  } else {
    console.warn(
      `⚠️ Candidate email update skipped or not required for candidate_id: ${oldCandidate._id}`
    );
  }
}

// *************** Send STUD_REG_N1 notification to reset recovery code
try {
  await CandidateUtility.Send_STUD_REG_N1(oldCandidate._id, lang);
  console.log(`✅ STUD_REG_N1 notification sent for candidate_id: ${oldCandidate._id}`);
} catch (err) {
  console.error(`⚠️ Failed to send STUD_REG_N1 notification for candidate_id: ${oldCandidate._id}`, err);
}

// *************** Validate selected payment plan consistency
if (
  oldSelectedPaymentPlanData?.total_amount > 0 &&
  typeof oldSelectedPaymentPlanData === 'object' &&
  typeof candidate_input?.selected_payment_plan === 'object'
) {
  const oldPlanEntries = Object.entries(oldSelectedPaymentPlanData);

  for (const [key, oldValue] of oldPlanEntries) {
    const newValue = candidate_input.selected_payment_plan[key];
    if (
      typeof oldValue !== 'object' &&
      String(newValue ?? '') !== String(oldValue ?? '')
    ) {
      throw new ApolloError('Payment plan is already selected!');
    }
  }
}

// *************** Ensure userId is always set (fallback to oldCandidate.user_id if no auth token)
if (!userId) {
  userId = oldCandidate?.user_id;
}

// *************** Admission process validation
if (!oldCandidate?.admission_process_id) {
  const paymentChanged =
    candidate_input?.payment_method &&
    candidate_input.payment_method !== oldCandidate?.payment_method;

  if (is_from_admission_form || paymentChanged) {
    await CandidateUtility.validateCandidateInput(candidate_input, oldCandidate);

    const restrictedStatuses = [
      'registered',
      'engaged',
      'resigned_after_engaged',
      'resigned_after_registered',
    ];

    if (restrictedStatuses.includes(oldCandidate?.candidate_admission_status)) {
      const currentStep = await CandidateUtility.getCandidateCurrentStep(oldCandidate);

      if (!candidate_input?.payment_method && currentStep !== 'down_payment') {
        throw new ApolloError('Cannot edit data, candidate already signed school contract!');
      }
    }
  }
}

  // *************** Handle candidate status update and readmission table sync
if (candidate_input?.candidate_admission_status) {
  const oldStatus = oldCandidate?.candidate_admission_status;
  const newStatus = candidate_input.candidate_admission_status;

  const resignStatuses = [
    'resigned',
    'resigned_after_engaged',
    'resigned_after_registered',
    'no_show',
    'resignation_missing_prerequisites',
    'resign_after_school_begins',
    'report_inscription',
  ];

  const isRegisteredToResigned =
    oldStatus === 'registered' && resignStatuses.includes(newStatus);

  const isResignedToRegistered =
    newStatus === 'registered' && resignStatuses.includes(oldStatus);

  if (isRegisteredToResigned || isResignedToRegistered) {
    // *************** Find all candidate IDs with same student_id
    const candidateFound = await CandidateModel.findById(oldCandidate._id)
      .select('student_id')
      .lean();

    if (candidateFound?.student_id) {
      const otherCandidates = await CandidateModel.find({
        student_id: candidateFound.student_id,
      })
        .select('_id')
        .lean();

      const candidateIds = otherCandidates.map((c) => c._id);

      // *************** Update readmission records
      const updateData = {
        $set: {
          is_student_resigned: isRegisteredToResigned,
        },
      };

      await CandidateModel.updateMany(
        {
          _id: { $in: candidateIds },
          readmission_status: 'assignment_table',
        },
        updateData
      );

      console.log(
        `✅ Updated readmission_status for student_id: ${candidateFound.student_id} → resigned = ${isRegisteredToResigned}`
      );
    }
  }
}

// *************** Start process: change step status for continuous formation student
const typeOfFormation = await TypeOfFormationModel.findById(
  oldCandidate?.type_of_formation_id
).lean();

const continuousFormationTypes = [
  'continuous',
  'continuous_total_funding',
  'continuous_partial_funding',
  'continuous_personal_funding',
  'continuous_contract_pro',
];

let admissionProcess = null;

// *************** Only proceed if candidate has an admission process
if (oldCandidate?.admission_process_id) {
  admissionProcess = await FormProcessModel.findById(
    oldCandidate.admission_process_id
  )
    .populate([
      {
        path: 'steps form_builder_id',
        populate: { path: 'steps' },
      },
      {
        path: 'candidate_id',
        populate: { path: 'continuous_formation_manager_id' },
      },
    ])
    .exec();

  // *************** If payment method changed to cash, accept down payment step automatically
  if (
    candidate_input?.payment_method === 'cash' &&
    admissionProcess?.steps?.length > 0
  ) {
    const downPaymentStep = admissionProcess.steps.find(
      (step) => step.step_type === 'down_payment_mode'
    );

    if (downPaymentStep?._id) {
      try {
        await FormProcessStepModel.findByIdAndUpdate(downPaymentStep._id, {
          $set: { step_status: 'accept' },
        });

        await CandidateUtility.updateCandidateAdmissionStatusFromAdmissionProcessStep(
          _id,
          downPaymentStep._id,
          context.userId,
          lang
        );

        await StudentAdmissionProcessUtilities.validateStatusStepFinalMessage(
          admissionProcess._id
        );

        console.log(
          `Step '${downPaymentStep.step_type}' accepted automatically for candidate_id: ${_id}`
        );
      } catch (err) {
        console.error(
          `Failed to auto-accept down_payment_mode step for candidate_id: ${_id}`,
          err
        );
      }
    }
  }
}

  // *************** Process to auto-accept step 'down_payment_mode'
const isContinuousFormation =
  typeOfFormation &&
  continuousTypeOfFormation.includes(typeOfFormation.type_of_formation);

const isReadmission = oldCandidate?.readmission_status === 'readmission_table';

const isPaymentMethodChanged =
  candidate_input?.payment_method &&
  candidate_input.payment_method !== oldCandidate?.payment_method;

const isExcludedPaymentMethod = ['credit_card', 'sepa', 'transfer', 'check', 'bank'].includes(
  candidate_input?.payment_method
);

const isNoDownPayment = candidate_input?.payment === 'no_down_payment';

// *************** If continuous formation or readmission and payment method allows auto-accept
if (
  ((isContinuousFormation || isReadmission) &&
    isPaymentMethodChanged &&
    !isExcludedPaymentMethod) ||
  isNoDownPayment
) {
  const downPaymentStep = admissionProcess?.steps?.find(
    (step) => step.step_type === 'down_payment_mode'
  );

  if (downPaymentStep?._id) {
    try {
      // *************** Accept the down payment step
      await FormProcessStepModel.findByIdAndUpdate(downPaymentStep._id, {
        $set: { step_status: 'accept' },
      });

      // *************** Update candidate admission status from this step
      await CandidateUtility.updateCandidateAdmissionStatusFromAdmissionProcessStep(
        _id,
        downPaymentStep._id,
        context.userId,
        lang
      );

      // *************** Auto-register candidate if formation type is 'classic'
      if (typeOfFormation?.type_of_formation === 'classic') {
        await CandidateUtility.proceedRegisteredStudent(_id, lang);
        console.log(`Candidate ${_id} auto-registered (classic formation).`);
      } else {
        console.log(`Down payment step auto-accepted for candidate ${_id}.`);
      }
    } catch (err) {
      console.error(
        `Failed to auto-accept down payment step for candidate ${_id}:`,
        err
      );
    }
  }
}

  // *************** Process to auto-accept step 'campus_validation'
const isContinuousFormation =
  typeOfFormation &&
  continuousTypeOfFormation.includes(typeOfFormation.type_of_formation);

const isReadmission = oldCandidate?.readmission_status === 'readmission_table';
const isProgramConfirmed =
  candidate_input?.program_confirmed === 'done';

if ((isContinuousFormation || isReadmission) && isProgramConfirmed) {
  const campusStep = admissionProcess?.steps?.find(
    (step) => step.step_type === 'campus_validation'
  );

  if (campusStep?._id) {
    try {
      // *************** Accept the campus validation step
      await FormProcessStepModel.findByIdAndUpdate(campusStep._id, {
        $set: { step_status: 'accept' },
      });

      // *************** Update candidate admission status based on this step
      await CandidateUtility.updateCandidateAdmissionStatusFromAdmissionProcessStep(
        _id,
        campusStep._id,
        context.userId,
        lang
      );

      console.log(`Campus validation step auto-accepted for candidate_id: ${_id}`);
    } catch (err) {
      console.error(
        `Failed to auto-accept campus_validation step for candidate_id: ${_id}`,
        err
      );
    }
  }
}
// *************** Process to auto-accept step 'school_contract' when signature is done
const isContinuousFormation =
  typeOfFormation &&
  continuousTypeOfFormation.includes(typeOfFormation.type_of_formation);

const isReadmission = oldCandidate?.readmission_status === 'readmission_table';
const isSignatureDone = candidate_input?.signature === 'done';

if ((isContinuousFormation || isReadmission) && isSignatureDone) {
  const summaryStep = admissionProcess?.steps?.find(
    (step) => step.step_type === 'summary'
  );

  if (summaryStep?._id) {
    try {
      // *************** Accept school contract step
      await FormProcessStepModel.findByIdAndUpdate(
        summaryStep._id,
        { $set: { step_status: 'accept' } },
        { new: true }
      );

      // *************** Update candidate admission status
      await CandidateUtility.updateCandidateAdmissionStatusFromAdmissionProcessStep(
        _id,
        summaryStep._id,
        context.userId,
        lang
      );

      // *************** Update signature date in FormProcess and Candidate
      const nowDate = nowTime.format('DD/MM/YYYY');
      const nowHour = nowTime.format('HH:mm');

      await FormProcessModel.findByIdAndUpdate(oldCandidate.admission_process_id, {
        $set: {
          signature_date: { date: nowDate, time: nowHour },
        },
      });

      candidate_input.candidate_sign_date = { date: nowDate, time: nowHour };

      // *************** Generate school contract PDF and save link
      const summarySchoolPdf = await StudentAdmissionProcessUtility.generatePDFStep(
        _id,
        summaryStep._id,
        lang
      );
      candidate_input.school_contract_pdf_link = summarySchoolPdf;

      console.log(`School contract accepted and PDF generated for candidate_id: ${_id}`);
    } catch (err) {
      console.error(`Failed to process school contract for candidate_id: ${_id}`, err);
    }
  }
}

// *************** Handle payment method logic
if (
  candidate_input?.payment_method &&
  ['check', 'transfer'].includes(candidate_input.payment_method) &&
  oldCandidate?.payment === 'not_authorized'
) {
  candidate_input.payment = 'not_done';
}

// *************** Prevent downgrading payment status
if (oldCandidate?.payment === 'done' && candidate_input?.payment === 'pending') {
  candidate_input.payment = oldCandidate.payment;
}

// *************** Keep same payment status if method not changed
if (
  candidate_input?.payment_method &&
  oldCandidate?.payment_method === candidate_input.payment_method
) {
  candidate_input.payment = oldCandidate.payment;
}

// *************** Validate IBAN if finance changed to 'my_self'
if (
  candidate_input?.finance &&
  oldCandidate?.finance !== candidate_input.finance &&
  candidate_input.finance === 'my_self'
) {
  // Skip validation if save from student card (identity student)
  if (oldCandidate?.method_of_payment === 'sepa' && !is_save_identity_student) {
    if (
      !candidate_input?.iban ||
      !candidate_input?.bic ||
      !candidate_input?.account_holder_name
    ) {
      throw new ApolloError('Answer of question is required');
    }

    const checkIban = await IbanHistoryModel.findOne({
      candidate_id: oldCandidate._id,
    })
      .sort({ _id: -1 })
      .lean();

    if (!checkIban || checkIban.message !== 'success') {
      throw new ApolloError('IBAN not verified');
    }
  }
}
// *************** Process to auto-accept step 'school_contract' when signature is done
const isContinuousFormation =
  typeOfFormation &&
  continuousTypeOfFormation.includes(typeOfFormation.type_of_formation);

const isReadmission = oldCandidate?.readmission_status === 'readmission_table';
const isSignatureDone = candidate_input?.signature === 'done';

if ((isContinuousFormation || isReadmission) && isSignatureDone) {
  const summaryStep = admissionProcess?.steps?.find(
    (step) => step.step_type === 'summary'
  );

  if (summaryStep?._id) {
    try {
      // *************** Accept school contract step
      await FormProcessStepModel.findByIdAndUpdate(
        summaryStep._id,
        { $set: { step_status: 'accept' } },
        { new: true }
      );

      // *************** Update candidate admission status
      await CandidateUtility.updateCandidateAdmissionStatusFromAdmissionProcessStep(
        _id,
        summaryStep._id,
        context.userId,
        lang
      );

      // *************** Update signature date in FormProcess and Candidate
      const nowDate = nowTime.format('DD/MM/YYYY');
      const nowHour = nowTime.format('HH:mm');

      await FormProcessModel.findByIdAndUpdate(oldCandidate.admission_process_id, {
        $set: {
          signature_date: { date: nowDate, time: nowHour },
        },
      });

      candidate_input.candidate_sign_date = { date: nowDate, time: nowHour };

      // *************** Generate school contract PDF and save link
      const summarySchoolPdf = await StudentAdmissionProcessUtility.generatePDFStep(
        _id,
        summaryStep._id,
        lang
      );
      candidate_input.school_contract_pdf_link = summarySchoolPdf;

      console.log(`✅ School contract accepted and PDF generated for candidate_id: ${_id}`);
    } catch (err) {
      console.error(`⚠️ Failed to process school contract for candidate_id: ${_id}`, err);
    }
  }
}

// *************** Handle payment method logic
if (
  candidate_input?.payment_method &&
  ['check', 'transfer'].includes(candidate_input.payment_method) &&
  oldCandidate?.payment === 'not_authorized'
) {
  candidate_input.payment = 'not_done';
}

// *************** Prevent downgrading payment status
if (oldCandidate?.payment === 'done' && candidate_input?.payment === 'pending') {
  candidate_input.payment = oldCandidate.payment;
}

// *************** Keep same payment status if method not changed
if (
  candidate_input?.payment_method &&
  oldCandidate?.payment_method === candidate_input.payment_method
) {
  candidate_input.payment = oldCandidate.payment;
}

// *************** Validate IBAN if finance changed to 'my_self'
if (
  candidate_input?.finance &&
  oldCandidate?.finance !== candidate_input.finance &&
  candidate_input.finance === 'my_self'
) {
  // Skip validation if save from student card (identity student)
  if (oldCandidate?.method_of_payment === 'sepa' && !is_save_identity_student) {
    if (
      !candidate_input?.iban ||
      !candidate_input?.bic ||
      !candidate_input?.account_holder_name
    ) {
      throw new ApolloError('Answer of question is required');
    }

    const checkIban = await IbanHistoryModel.findOne({
      candidate_id: oldCandidate._id,
    })
      .sort({ _id: -1 })
      .lean();

    if (!checkIban || checkIban.message !== 'success') {
      throw new ApolloError('IBAN not verified');
    }
  }
}

// *************** Failsafe: keep only valid parents with required data
if (Array.isArray(candidate_input?.parents) && candidate_input.parents.length > 0) {
  candidate_input.parents = candidate_input.parents.filter((parent) => {
    const hasRequiredFields =
      parent?.family_name?.trim() &&
      parent?.name?.trim() &&
      parent?.email?.trim();

    return Boolean(hasRequiredFields);
  });
}

// *************** Failsafe: keep only valid payment supports with required data
if (Array.isArray(candidate_input?.payment_supports) && candidate_input.payment_supports.length > 0) {
  candidate_input.payment_supports = candidate_input.payment_supports.filter((support) => {
    const hasRequiredFields =
      support?.family_name?.trim() &&
      support?.name?.trim() &&
      support?.email?.trim();

    return Boolean(hasRequiredFields);
  });
}

// *************** Save legal representative history
if (candidate_input?.legal_representative) {
  try {
    await CandidateUtility.SaveHistoryLegalRepresentative(candidate_input, _id, userId);
    console.log(`✅ Legal representative history saved for candidate_id: ${_id}`);
  } catch (err) {
    console.error(
      `⚠️ Failed to save legal representative history for candidate_id: ${_id}`,
      err
    );
  }
}

// *************** Sync CVEC and INE numbers from student card to CVEC forms
if (candidate_input?.cvec_number || candidate_input?.ine_number) {
  // *************** Normalize to uppercase
  if (candidate_input.cvec_number) {
    candidate_input.cvec_number = candidate_input.cvec_number.toUpperCase().trim();
  }
  if (candidate_input.ine_number) {
    candidate_input.ine_number = candidate_input.ine_number.toUpperCase().trim();
  }

  try {
    // *************** Get all CVEC form processes for this candidate
    let cvecFormProcesses = [];

    if (oldCandidate?.cvec_form_process_id) {
      const singleProcess = await FormProcessModel.findById(oldCandidate.cvec_form_process_id)
        .populate([{ path: 'steps', populate: [{ path: 'segments.questions' }] }])
        .lean();
      if (singleProcess) cvecFormProcesses.push(singleProcess);
    } else {
      const formBuilderIds = await FormBuilderModel.distinct('_id', {
        status: 'active',
        template_type: 'one_time_form',
      });

      cvecFormProcesses = await FormProcessModel.find({
        status: 'active',
        candidate_id: oldCandidate._id,
        form_builder_id: { $in: formBuilderIds },
      })
        .populate([{ path: 'steps', populate: [{ path: 'segments.questions' }] }])
        .lean();
    }

    // *************** Iterate and update CVEC/INE answers
    for (const process of cvecFormProcesses) {
      for (const step of process.steps || []) {
        if (step.step_type !== 'question_and_field' || step.step_status !== 'accept') continue;

        for (const segment of step.segments || []) {
          for (const question of segment.questions || []) {
            const fieldType = question?.field_type;
            const answer = question?.answer?.toString().trim().toUpperCase() || '';

            if (fieldType === 'cvec_number' && candidate_input.cvec_number && answer !== candidate_input.cvec_number) {
              await FormProcessQuestionModel.findByIdAndUpdate(
                question._id,
                { $set: { answer: candidate_input.cvec_number } },
                { new: true }
              );
              console.log(`✅ Updated CVEC number in form for candidate_id: ${_id}`);
            }

            if (fieldType === 'ine_number' && candidate_input.ine_number && answer !== candidate_input.ine_number) {
              await FormProcessQuestionModel.findByIdAndUpdate(
                question._id,
                { $set: { answer: candidate_input.ine_number } },
                { new: true }
              );
              console.log(`✅ Updated INE number in form for candidate_id: ${_id}`);
            }
          }
        }
      }
    }
  } catch (err) {
    console.error(`⚠️ Failed to sync CVEC/INE numbers for candidate_id: ${_id}`, err);
  }
}

// *************** Update candidate record with new input
const updatedCandidate = await CandidateModel.findByIdAndUpdate(
  _id,
  { $set: candidate_input },
  { new: true }
);

// *************** Process to auto-accept step 'scholarship_fee' when payment plan changes
try {
  const oldSelectedPaymentPlan = oldCandidate?.selected_payment_plan || {};
  if (Array.isArray(oldSelectedPaymentPlan.payment_date)) {
    oldSelectedPaymentPlan.payment_date = oldSelectedPaymentPlan.payment_date.map((term) => {
      const { _id, ...rest } = term;
      return rest;
    });
  }

  // *************** Compare with new payment plan input
  if (
    candidate_input?.selected_payment_plan &&
    typeof candidate_input.selected_payment_plan === 'object'
  ) {
    const paymentPlanChanged =
      JSON.stringify(oldSelectedPaymentPlan) !==
      JSON.stringify(candidate_input.selected_payment_plan);

    if (paymentPlanChanged) {
      // *************** Continuous formation or readmission process
      const isContinuousFormation =
        typeOfFormation &&
        continuousTypeOfFormation.includes(typeOfFormation.type_of_formation);
      const isReadmission =
        oldCandidate?.readmission_status === 'readmission_table';

      if ((isContinuousFormation || isReadmission) && admissionProcess?.steps?.length) {
        const scholarshipStep = admissionProcess.steps.find(
          (step) => step.step_type === 'scholarship_fee'
        );

        if (scholarshipStep?._id) {
          await FormProcessStepModel.findByIdAndUpdate(
            scholarshipStep._id,
            { $set: { step_status: 'accept' } },
            { new: true }
          );

          await CandidateUtility.updateCandidateAdmissionStatusFromAdmissionProcessStep(
            _id,
            scholarshipStep._id,
            context.userId,
            lang
          );

          console.log(`✅ Scholarship fee step auto-accepted for candidate_id: ${_id}`);
        }
      }
    }
  }
} catch (err) {
  console.error(
    `Failed to auto-accept scholarship_fee step for candidate_id: ${_id}`,
    err
  );
}

// *************** Admission FI: update billing if selected payment plan changed
if (
  typeOfFormation?.type_of_formation === 'classic' &&
  !oldCandidate?.readmission_status
) {
  let updateFinance = false;

  // Pastikan data payment plan valid sebelum membandingkan
  if (
    candidate_input?.selected_payment_plan &&
    typeof candidate_input.selected_payment_plan === 'object' &&
    oldSelectedPaymentPlanData &&
    typeof oldSelectedPaymentPlanData === 'object'
  ) {
    // Bandingkan field per field antara payment plan lama dan baru
    for (const [key, newValue] of Object.entries(candidate_input.selected_payment_plan)) {
      const oldValue = oldSelectedPaymentPlanData[key];
      if (String(newValue ?? '') !== String(oldValue ?? '')) {
        updateFinance = true;
        break; // cukup satu perubahan, langsung tandai update
      }
    }
  }

  // *************** Jalankan update billing jika ada perubahan
  if (updateFinance) {
    try {
      await CandidateUtility.updateCandidateBilling(
        oldCandidate,
        updatedCandidate,
        context.userId
      );
      console.log(`✅ Billing updated for candidate_id: ${_id}`);
    } catch (err) {
      console.error(
        `Failed to update billing for candidate_id: ${_id}`,
        err
      );
    }
  } else {
    console.log(`No billing update required for candidate_id: ${_id}`);
  }
}

// *************** Generate billing if scholarship fee step is accepted
try {
  const admissionProcessUpdated = await FormProcessModel.findById(
    updatedCandidate.admission_process_id
  )
    .populate({ path: 'steps' })
    .lean();

  // *************** Pastikan data proses dan step valid
  if (admissionProcessUpdated?.steps?.length) {
    const scholarshipStep = admissionProcessUpdated.steps.find(
      (step) => step.step_type === 'scholarship_fee'
    );

    // *************** Cek kondisi untuk generate billing ulang
    if (
      typeOfFormation &&
      (
        continuousTypeOfFormation.includes(typeOfFormation.type_of_formation) ||
        oldCandidate.readmission_status === 'readmission_table'
      ) &&
      scholarshipStep &&
      scholarshipStep.step_status === 'accept' &&
      oldScholarshipStep &&
      oldScholarshipStep.step_status !== 'accept'
    ) {
      await CandidateUtility.updateCandidateBilling(
        oldCandidate,
        updatedCandidate,
        context.userId
      );
      console.log(`✅ Billing regenerated for candidate_id: ${_id}`);
    } else {
      console.log(`ℹ️ No billing regeneration needed for candidate_id: ${_id}`);
    }
  }
} catch (err) {
  console.error(
    `⚠️ Error while regenerating billing for candidate_id: ${_id}`,
    err
  );
}
  // *************** Update user data based on updated candidate information
let userCandidate = await UserModel.findById(updatedCandidate.user_id);

if (userCandidate) {
  // *************** Pastikan field user_addresses valid
  if (!Array.isArray(userCandidate.user_addresses)) {
    userCandidate.user_addresses = [];
  }

  // *************** Update atau buat alamat pertama user
  userCandidate.user_addresses[0] = {
    address: updatedCandidate.address || '',
    postal_code: updatedCandidate.post_code || '',
    country: updatedCandidate.country || '',
    city: updatedCandidate.city || '',
    department: updatedCandidate.department || '',
    region: updatedCandidate.region || '',
  };

  try {
    await userCandidate.save();
    console.log(`✅ User address updated for user_id: ${userCandidate._id}`);
  } catch (err) {
    console.error(`⚠️ Failed to update user address for user_id: ${userCandidate._id}`, err);
  }
} else {
  console.log(`ℹ️ No user found for candidate_id: ${updatedCandidate._id}`);
}
  // *************** Sync updated candidate data to related user
if (updatedCandidate?.user_id) {
  try {
    await UserModel.findByIdAndUpdate(
      updatedCandidate.user_id,
      {
        $set: {
          last_name: updatedCandidate.last_name || '',
          first_name: updatedCandidate.first_name || '',
          civility: updatedCandidate.civility || '',
          sex:
            updatedCandidate.civility === 'neutral'
              ? 'N'
              : updatedCandidate.sex || '',
          user_addresses:
            (userCandidate && Array.isArray(userCandidate.user_addresses)
              ? userCandidate.user_addresses
              : []),
          email: updatedCandidate.email || '',
          portable_phone: updatedCandidate.telephone || '',
          office_phone: updatedCandidate.fixed_phone || '',
        },
      },
      { new: true }
    );

    console.log(`✅ User data synced successfully for user_id: ${updatedCandidate.user_id}`);
  } catch (err) {
    console.error(`⚠️ Failed to sync user data for user_id: ${updatedCandidate.user_id}`, err);
  }
} else {
  console.log(`ℹ️ No user_id found for candidate_id: ${updatedCandidate._id}`);
}

  // *************** Create new candidate history
await CandidateHistoryUtility.createNewCandidateHistory(_id, userId, 'update_candidate');

// *************** Update mentor flag if needed
if (candidate_input.student_mentor_id && updatedCandidate.student_mentor_id) {
  await StudentModel.updateOne(
    { _id: updatedCandidate.student_mentor_id },
    { $set: { is_candidate_mentor: true } }
  );
}

const bulkUpdateCandidateQuery = [];
let oldAdmissionMemberId;

// *************** Admission member change detection
if (
  candidate_input.admission_member_id &&
  String(oldCandidate.admission_member_id) !== String(updatedCandidate.admission_member_id)
) {
  oldAdmissionMemberId = oldCandidate.admission_member_id;

  if (!userId) {
    await CandidateModel.updateOne({ _id }, { $set: oldCandidate });
    throw new AuthenticationError('Authorization header is missing');
  }

  // *************** Update old admission member history & push new one
  bulkUpdateCandidateQuery.push(
    {
      updateOne: {
        filter: {
          _id,
          'admission_member_histories.admission_member_status': 'active',
          'admission_member_histories.admission_member_id': mongoose.Types.ObjectId(oldCandidate.admission_member_id),
        },
        update: {
          $set: {
            'admission_member_histories.$.admission_member_status': 'not_active',
            'admission_member_histories.$.deactivation_date': nowTime.format('DD/MM/YYYY'),
            'admission_member_histories.$.deactivation_time': nowTime.format('HH:mm'),
          },
        },
      },
    },
    {
      updateOne: {
        filter: { _id },
        update: {
          $push: {
            admission_member_histories: {
              admission_member_id: candidate_input.admission_member_id,
              activation_date: nowTime.format('DD/MM/YYYY'),
              activation_time: nowTime.format('HH:mm'),
            },
          },
        },
      },
    }
  );

  await CandidateHistoryUtility.createNewCandidateHistory(
    _id,
    userId,
    'update_candidate_admission_member',
    `Admission member updated from ${oldCandidate.admission_member_id} to ${updatedCandidate.admission_member_id}`
  );

  // *************** Send notification
  await CandidateUtility.send_CANDIDATE_N2(
    [updatedCandidate],
    lang,
    userId,
    [null, ''].includes(oldCandidate.admission_member_id)
  );

  if (oldCandidate.admission_member_id) {
    await CandidateUtility.send_CANDIDATE_N6([oldCandidate], lang, userId);
  }
}

// *************** Student mentor change detection
if (
  candidate_input.student_mentor_id &&
  String(oldCandidate.student_mentor_id) !== String(updatedCandidate.student_mentor_id)
) {
  if (!userId) {
    await CandidateModel.updateOne({ _id }, { $set: oldCandidate });
    throw new AuthenticationError('Authorization header is missing');
  }

  bulkUpdateCandidateQuery.push(
    {
      updateOne: {
        filter: {
          _id,
          'student_mentor_histories.student_mentor_status': 'active',
          'student_mentor_histories.student_mentor_id': mongoose.Types.ObjectId(oldCandidate.student_mentor_id),
        },
        update: {
          $set: {
            'student_mentor_histories.$.student_mentor_status': 'not_active',
            'student_mentor_histories.$.deactivation_date': nowTime.format('DD/MM/YYYY'),
            'student_mentor_histories.$.deactivation_time': nowTime.format('HH:mm'),
          },
        },
      },
    },
    {
      updateOne: {
        filter: { _id },
        update: {
          $push: {
            student_mentor_histories: {
              student_mentor_id: candidate_input.student_mentor_id,
              activation_date: nowTime.format('DD/MM/YYYY'),
              activation_time: nowTime.format('HH:mm'),
            },
          },
        },
      },
    }
  );

  await CandidateHistoryUtility.createNewCandidateHistory(
    _id,
    userId,
    'update_candidate_student_mentor_id',
    `Student mentor updated from ${oldCandidate.student_mentor_id} to ${updatedCandidate.student_mentor_id}`
  );

  // *************** Notifications
  if (oldCandidate.student_mentor_id) {
    await CandidateUtility.send_CANDIDATE_N4([oldCandidate], lang, userId);
  }

  await CandidateUtility.send_CANDIDATE_N3([updatedCandidate], lang, userId);
  await CandidateUtility.send_CANDIDATE_N5([updatedCandidate], lang, userId);
}

// *************** Campus change detection
if (candidate_input.campus && String(oldCandidate.campus) !== String(updatedCandidate.campus)) {
  if (!userId) {
    await CandidateModel.updateOne({ _id }, { $set: oldCandidate });
    throw new AuthenticationError('Authorization header is missing');
  }

  await CandidateModel.updateOne({ _id }, { $set: { campus: oldCandidate.campus } });

  bulkUpdateCandidateQuery.push({
    updateOne: {
      filter: {
        _id,
        campus_histories: {
          $not: {
            $elemMatch: {
              campus: candidate_input.campus,
              campus_status: 'pending',
            },
          },
        },
      },
      update: {
        $push: {
          campus_histories: {
            campus: candidate_input.campus,
            campus_status: 'pending',
          },
        },
      },
    },
  });

  await CandidateHistoryUtility.createNewCandidateHistory(
    _id,
    userId,
    'update_candidate_campus',
    `Campus updated from ${oldCandidate.campus} to ${updatedCandidate.campus}`
  );
}

// *************** Handle engagement level change
if (
  candidate_input.engagement_level &&
  oldCandidate.engagement_level !== 'registered' &&
  updatedCandidate.engagement_level === 'registered'
) {
  await CandidateUtility.addRegisteredCandidateAsStudent({
    candidate: updatedCandidate,
    isSentStudRegN1: false,
    lang,
  });

  if (oldCandidate.candidate_admission_status !== 'resign_after_school_begins') {
    await CandidateUtility.send_REGISTRATION_N3(updatedCandidate);
  }

  if (!updatedCandidate.is_registration_recorded) {
    await GeneralDashboardAdmissionUtility.recordCandidateRegistered(updatedCandidate, userId);
  }

  await CandidateHistoryUtility.createNewCandidateHistory(
    _id,
    userId,
    'update_candidate_engagement_level',
    `Candidate ${updatedCandidate._id} registered`
  );
}

// *************** Handle admission status transition to 'registered'
if (
  candidate_input.candidate_admission_status &&
  oldCandidate.candidate_admission_status !== 'registered' &&
  updatedCandidate.candidate_admission_status === 'registered'
) {
  await CandidateUtility.addRegisteredCandidateAsStudent({ candidate: updatedCandidate, lang });

  const countDocs = await CandidateModel.countDocuments({
    program_status: 'active',
    $or: [
      { _id: updatedCandidate._id },
      { email: updatedCandidate.email },
      { user_id: updatedCandidate.user_id },
    ],
  });
}

  // *************** If there are no active student for this candidate
// **************** RA_EDH_0188: Keep creating readmission assignment student if not exist in assignment table
try {
  const checkResult = await CandidateUtility.CheckCandidateExistInReadmission(updatedCandidate);

  if (!checkResult) {
    const scholarSeason = await ScholarSeasonModel.findById(
      updatedCandidate.scholar_season
    ).lean();

    if (scholarSeason?.from?.date_utc && scholarSeason?.to?.date_utc) {
      const startDate = moment(scholarSeason.from.date_utc, 'DD/MM/YYYY');
      const finishDate = moment(scholarSeason.to.date_utc, 'DD/MM/YYYY');
      const today = moment().utc();

      // *************** Only activate program if today is within scholar season range
      if (today.isSameOrAfter(startDate) && today.isSameOrBefore(finishDate)) {
        await CandidateModel.findByIdAndUpdate(updatedCandidate._id, {
          $set: { program_status: 'active' },
        });
        console.log(`Candidate program activated for candidate_id: ${updatedCandidate._id}`);
      } else {
        console.log(
          `Candidate not within scholar season range, skipping activation for candidate_id: ${updatedCandidate._id}`
        );
      }
    } else {
      console.warn(
        `Scholar season date missing for candidate_id: ${updatedCandidate._id}`
      );
    }

    // *************** Create next candidate data (readmission)
    await CandidateUtility.createNextCandidateData(updatedCandidate);
    console.log(`✅ Next candidate data created for candidate_id: ${updatedCandidate._id}`);
  } else {
    console.log(`ℹ️ Candidate already exists in readmission table: ${updatedCandidate._id}`);
  }
} catch (err) {
  console.error(
    `⚠️ Error while checking or creating readmission for candidate_id: ${updatedCandidate?._id}`,
    err
  );
}
// *************** Ensure assignment table entry exists and send registration notifications
try {
  // Prevent duplicate assignment creation
  await CandidateUtility.checkAndCreateCandidateAssignmentTable(updatedCandidate._id);

  // Send REGISTRATION_N7 only for initial formation (not continuous) and not readmission
  if (
    typeOfFormation &&
    !continuousTypeOfFormation.includes(typeOfFormation.type_of_formation) &&
    updatedCandidate.readmission_status !== 'readmission_table'
  ) {
    await CandidateUtility.send_REGISTRATION_N7(updatedCandidate, lang, is_prevent_resend_notif);
  } else if (updatedCandidate.readmission_status === 'readmission_table') {
    // Send readmission registration notification
    await CandidateUtility.send_READ_REG_N7(updatedCandidate, lang, is_prevent_resend_notif);
  }

  // Update registered_at timestamp
  await CandidateModel.findByIdAndUpdate(updatedCandidate._id, {
    $set: {
      registered_at: {
        date: moment.utc().format('DD/MM/YYYY'),
        time: moment.utc().format('HH:mm'),
      },
    },
  });

  // Record registration to dashboard if not recorded
  if (!updatedCandidate.is_registration_recorded) {
    try {
      await GeneralDashboardAdmissionUtility.recordCandidateRegistered(updatedCandidate, userId);
    } catch (err) {
      console.error(`⚠️ Failed to record candidate registration for ${updatedCandidate._id}`, err);
    }
  }

  await CandidateHistoryUtility.createNewCandidateHistory(
    _id,
    userId,
    'update_candidate_campus',
    `Candidate ${updatedCandidate._id} registered`
  );

  // Refund when moving from 'report_inscription' -> 'registered'
  if (oldCandidate.candidate_admission_status === 'report_inscription' && updatedCandidate.candidate_admission_status === 'registered') {
    try {
      await CandidateUtility.refundTransanctionHistoryOfCandidate(oldCandidate, updatedCandidate, userId);
    } catch (err) {
      console.error(`⚠️ Failed to refund transaction history for ${updatedCandidate._id}`, err);
    }
  }

  // Restore closed CVEC form if candidate moved from resigned_after_registered -> registered
  if (
    oldCandidate.closed_cvec_form_process_id &&
    oldCandidate.candidate_admission_status === 'resigned_after_registered'
  ) {
    try {
      await FormProcessModel.findByIdAndUpdate(oldCandidate.closed_cvec_form_process_id, { $set: { is_form_closed: false } });
      await CandidateModel.findByIdAndUpdate(oldCandidate._id, {
        $set: {
          cvec_form_process_id: oldCandidate.closed_cvec_form_process_id,
          closed_cvec_form_process_id: undefined,
        },
      });
    } catch (err) {
      console.error(`⚠️ Failed to restore CVEC form for ${oldCandidate._id}`, err);
    }
  }

  // Restore closed admission document similarly
  if (
    oldCandidate.closed_admission_document_process_id &&
    oldCandidate.candidate_admission_status === 'resigned_after_registered'
  ) {
    try {
      await FormProcessModel.findByIdAndUpdate(oldCandidate.closed_admission_document_process_id, { $set: { is_form_closed: false } });
      await CandidateModel.findByIdAndUpdate(oldCandidate._id, {
        $set: {
          admission_document_process_id: oldCandidate.closed_admission_document_process_id,
          closed_admission_document_process_id: undefined,
        },
      });
    } catch (err) {
      console.error(`⚠️ Failed to restore admission document form for ${oldCandidate._id}`, err);
    }
  }
} catch (err) {
  console.error(`⚠️ Error during post-registration processing for candidate ${updatedCandidate?._id}`, err);
}

// *************** Handle transition to 'engaged' (auto-register for some profiles)
if (
  updatedCandidate.candidate_admission_status &&
  oldCandidate.candidate_admission_status !== 'engaged' &&
  updatedCandidate.candidate_admission_status === 'engaged' &&
  typeOfFormation &&
  (!continuousTypeOfFormation.includes(typeOfFormation.type_of_formation) || oldCandidate.readmission_status !== 'readmission_table')
) {
  try {
    if (updatedCandidate.registration_profile) {
      const profileRateCandidate = await ProfileRateModel.findById(mongoose.Types.ObjectId(updatedCandidate.registration_profile)).lean();
      if (profileRateCandidate && profileRateCandidate.is_down_payment === 'no') {
        await CandidateModel.findByIdAndUpdate(mongoose.Types.ObjectId(_id), {
          $set: {
            candidate_admission_status: 'registered',
            registered_at: {
              date: moment.utc().format('DD/MM/YYYY'),
              time: moment.utc().format('HH:mm'),
            },
          },
        });
      }
    }

    // Always set candidate sign date when moving to engaged
    await CandidateModel.updateOne(
      { _id },
      {
        $set: {
          candidate_sign_date: {
            date: moment.utc().format('DD/MM/YYYY'),
            time: moment.utc().format('HH:mm'),
          },
        },
      }
    );

    if (!oldCandidate.readmission_status) {
      try {
        await CandidateUtility.send_FORM_N1(updatedCandidate, lang);
      } catch (err) {
        console.error(`⚠️ Failed to send FORM_N1 for ${updatedCandidate._id}`, err);
      }
    }
  } catch (err) {
    console.error(`⚠️ Error handling engagement for candidate ${_id}`, err);
  }
}

// *************** Handle timestamps for various resignation statuses
try {
  if (
    candidate_input.candidate_admission_status &&
    oldCandidate.candidate_admission_status !== 'resigned' &&
    updatedCandidate.candidate_admission_status === 'resigned'
  ) {
    await CandidateModel.findByIdAndUpdate(updatedCandidate._id, {
      $set: {
        resigned_at: {
          date: moment.utc().format('DD/MM/YYYY'),
          time: moment.utc().format('HH:mm'),
        },
      },
    });
  }

  if (
    candidate_input.candidate_admission_status &&
    oldCandidate.candidate_admission_status !== 'resigned_after_engaged' &&
    updatedCandidate.candidate_admission_status === 'resigned_after_engaged'
  ) {
    await CandidateModel.findByIdAndUpdate(updatedCandidate._id, {
      $set: {
        resigned_after_engaged_at: {
          date: moment.utc().format('DD/MM/YYYY'),
          time: moment.utc().format('HH:mm'),
        },
      },
    });
  }

  if (
    candidate_input.candidate_admission_status &&
    oldCandidate.candidate_admission_status !== 'resigned_after_registered' &&
    updatedCandidate.candidate_admission_status === 'resigned_after_registered'
  ) {
    await CandidateModel.findByIdAndUpdate(updatedCandidate._id, {
      $set: {
        resigned_after_registered_at: {
          date: moment.utc().format('DD/MM/YYYY'),
          time: moment.utc().format('HH:mm'),
        },
      },
    });

    // update other mails on microsoft account if student exists
    const studentData = await StudentModel.findOne({ candidate_id: updatedCandidate._id }).lean();

    if (studentData?.microsoft_email) {
      const payload = {
        accountEnabled: false,
        mail: studentData.school_mail,
        givenName: studentData.first_name,
        surname: studentData.last_name,
        otherMails: [studentData.email],
        userPrincipalName: studentData.microsoft_email,
      };

      try {
        // *************** Uncomment when microsoftService token/domain is ready
        // await microsoftService.updateMicrosoftUser(payload);
      } catch (error) {
        console.warn('⚠️ microsoftService.updateMicrosoftUser failed', error);
      }
    }
  }
} catch (err) {
  console.error(`⚠️ Error updating resignation timestamps for ${updatedCandidate._id}`, err);
}
// *************** Close CVEC form if candidate status changed from registered to resigned_after_registered
try {
  if (
    oldCandidate?.candidate_admission_status === 'registered' &&
    oldCandidate?.cvec_form_process_id
  ) {
    const candidateCvecForm = await FormProcessModel.findById(
      oldCandidate.cvec_form_process_id
    )
      .select('steps')
      .populate([{ path: 'steps' }])
      .lean();

    if (candidateCvecForm?.steps?.length) {
      // *************** Check if any step still not started
      const hasNotStartedStep = candidateCvecForm.steps.some(
        (step) => step.step_status === 'not_started'
      );

      if (hasNotStartedStep) {
        // *************** Close CVEC form and move ID to closed_cvec_form_process_id
        await FormProcessModel.findByIdAndUpdate(oldCandidate.cvec_form_process_id, {
          $set: { is_form_closed: true },
        });

        await CandidateModel.findByIdAndUpdate(oldCandidate._id, {
          $set: {
            cvec_form_process_id: undefined,
            closed_cvec_form_process_id: oldCandidate.cvec_form_process_id,
          },
        });

        console.log(
          `Closed CVEC form for candidate_id: ${oldCandidate._id} (form_id: ${oldCandidate.cvec_form_process_id})`
        );
      } else {
        console.log(
          `No not_started steps found, CVEC form remains open for candidate_id: ${oldCandidate._id}`
        );
      }
    } else {
      console.warn(
        `No steps found for CVEC form of candidate_id: ${oldCandidate._id}`
      );
    }
  }
} catch (err) {
  console.error(
    `Error closing CVEC form for candidate_id: ${oldCandidate?._id}`,
    err
  );
}
// *************** Close Admission Document form if candidate status changed from registered to resigned_after_registered
try {
  if (
    oldCandidate?.candidate_admission_status === 'registered' &&
    oldCandidate?.admission_document_process_id
  ) {
    const candidateAdmissionDoc = await FormProcessModel.findById(
      oldCandidate.admission_document_process_id
    )
      .select('steps')
      .populate([{ path: 'steps' }])
      .lean();

    if (candidateAdmissionDoc?.steps?.length) {
      // *************** Check if any step still not started
      const hasNotStartedStep = candidateAdmissionDoc.steps.some(
        (step) => step.step_status === 'not_started'
      );

      if (hasNotStartedStep) {
        // *************** Close the admission document form
        await FormProcessModel.findByIdAndUpdate(
          oldCandidate.admission_document_process_id,
          { $set: { is_form_closed: true } }
        );

        // *************** Move form ID to closed_admission_document_process_id
        await CandidateModel.findByIdAndUpdate(oldCandidate._id, {
          $set: {
            admission_document_process_id: undefined,
            closed_admission_document_process_id:
              oldCandidate.admission_document_process_id,
          },
        });

        console.log(
          ` Closed Admission Document form for candidate_id: ${oldCandidate._id} (form_id: ${oldCandidate.admission_document_process_id})`
        );
      } else {
        console.log(
          `ℹ No not_started steps found, Admission Document form remains open for candidate_id: ${oldCandidate._id}`
        );
      }
    } else {
      console.warn(
        ` No steps found in Admission Document form for candidate_id: ${oldCandidate._id}`
      );
    }
  }
} catch (err) {
  console.error(
    `Error closing Admission Document form for candidate_id: ${oldCandidate?._id}`,
    err
  );
}

// *************** Handle candidate transfer request
try {
  if (
    candidate_input?.program_confirmed &&
    oldCandidate.program_confirmed !== 'request_transfer' &&
    updatedCandidate.program_confirmed === 'request_transfer'
  ) {
    await CandidateUtility.send_Transfer_N5(_id, new_desired_program, lang);
    await CandidateUtility.send_Transfer_N6(_id, new_desired_program, lang);

    console.log(
      ` Transfer notifications (N5, N6) sent for candidate_id: ${_id}`
    );
  }
} catch (err) {
  console.error(
    ` Failed to process transfer notifications for candidate_id: ${_id}`,
    err
  );
}

// *************** Handle candidate report inscription case
try {
  if (
    candidate_input?.candidate_admission_status &&
    oldCandidate.candidate_admission_status !== 'report_inscription' &&
    updatedCandidate.candidate_admission_status === 'report_inscription'
  ) {
    await CandidateUtility.refundTransanctionHistoryOfCandidate(
      oldCandidate,
      updatedCandidate,
      userId
    );

    await CandidateModel.findByIdAndUpdate(_id, {
      $set: {
        inscription_at: {
          date: moment.utc().format('DD/MM/YYYY'),
          time: moment.utc().format('HH:mm'),
        },
      },
    });

    await CandidateUtility.send_StudentCard_N1(updatedCandidate, lang);

    console.log(
      `Report inscription processed and StudentCard_N1 sent for candidate_id: ${_id}`
    );
  }
} catch (err) {
  console.error(
    ` Error handling report_inscription for candidate_id: ${_id}`,
    err
  );
}
// *************** Set bill_validated_at when candidate status changes to 'bill_validated'
try {
  if (
    oldCandidate?.candidate_admission_status !== 'bill_validated' &&
    updatedCandidate?.candidate_admission_status === 'bill_validated'
  ) {
    await CandidateModel.findByIdAndUpdate(_id, {
      $set: {
        bill_validated_at: {
          date: moment.utc().format('DD/MM/YYYY'),
          time: moment.utc().format('HH:mm'),
        },
      },
    });

    console.log(`✅ bill_validated_at timestamp set for candidate_id: ${_id}`);
  }
} catch (err) {
  console.error(
    `⚠️ Failed to set bill_validated_at for candidate_id: ${_id}`,
    err
  );
}


  // *************** Generate timestamps for key status changes
try {
  const now = {
    date: moment.utc().format('DD/MM/YYYY'),
    time: moment.utc().format('HH:mm'),
  };

  const statusTimeMap = [
    { field: 'bill_validated_at', status: 'bill_validated' },
    { field: 'financement_validated_at', status: 'financement_validated' },
    { field: 'mission_card_validated_at', status: 'mission_card_validated' },
    { field: 'in_scholarship_at', status: 'in_scholarship' },
    { field: 'resignation_missing_prerequisites_at', status: 'resignation_missing_prerequisites' },
  ];

  for (const { field, status } of statusTimeMap) {
    if (
      oldCandidate?.candidate_admission_status !== status &&
      updatedCandidate?.candidate_admission_status === status
    ) {
      await CandidateModel.findByIdAndUpdate(_id, { $set: { [field]: now } });
      console.log(` ${field} timestamp set for candidate_id: ${_id}`);
    }
  }
} catch (err) {
  console.error(` Failed to set status timestamps for candidate_id: ${_id}`, err);
}

// *************** Handle payment transitions
try {
  if (oldCandidate?.payment === 'pending' && !oldCandidate?.payment_method && candidate_input?.payment_method) {
    await CandidateModel.findByIdAndUpdate(updatedCandidate._id, { $set: { payment: 'pending' } });
  } else if (
    candidate_input?.payment_method &&
    oldCandidate?.payment_method !== updatedCandidate?.payment_method &&
    updatedCandidate?.payment !== 'done'
  ) {
    await CandidateModel.findByIdAndUpdate(updatedCandidate._id, { $set: { payment: 'not_done' } });

    if (
      typeOfFormation &&
      !continuousTypeOfFormation.includes(typeOfFormation.type_of_formation) &&
      oldCandidate.readmission_status !== 'readmission_table'
    ) {
      await CandidateUtility.send_FORM_N2(updatedCandidate, lang);
    }
  }

  if (bulkUpdateCandidateQuery.length > 0) {
    await CandidateModel.bulkWrite(bulkUpdateCandidateQuery);
  }

  await StudentAdmissionProcessUtility.updateStudentAdmissionProcessBasedOnStudentData(_id);

  // *************** Handle special case: payment method switched to cash
  if (candidate_input?.payment_method === 'cash' && oldCandidate?.payment_method !== 'cash') {
    const masterTransaction = await MasterTransactionModel.findOne({
      status: 'active',
      candidate_id: updatedCandidate._id,
      intake_channel: updatedCandidate.intake_channel,
      operation_name: { $in: ['payment_of_dp', 'down_payment'] },
      status_line_dp_term: 'billed',
    })
      .sort({ _id: -1 })
      .lean();

    if (masterTransaction) {
      await MasterTransactionModel.findByIdAndUpdate(masterTransaction._id, {
        $set: {
          nature: 'cash',
          method_of_payment: 'cash',
          status_line_dp_term: 'pending',
        },
      });

      await MasterTransactionUtilities.SaveMasterTransactionHistory(
        masterTransaction,
        '655ed03e608c5a450cea084e', // *************** user 'zetta' id for actor
        'UpdateCandidate',
        'generate_billing_admission'
      );

      candidate_input.payment = 'pending';
    }
  }
} catch (err) {
  console.error(` Failed to handle payment updates for candidate_id: ${_id}`, err);
}

  // *************** Auto-register candidate if signature just changed to "done"
try {
  if (oldCandidate.signature !== 'done' && updatedCandidate.signature === 'done') {
    if (updatedCandidate.billing_id) {
      const billing = await BillingModel.findById(updatedCandidate.billing_id).lean();

      if (billing && billing.amount_billed === 0 && billing.deposit_status === 'paid') {
        const candidateDataUpdated = await CandidateModel.findByIdAndUpdate(
          updatedCandidate._id,
          { $set: { candidate_admission_status: 'registered' } },
          { new: true }
        );

        // *************** Ensure candidate active in assignment table (RA_EDH_0188)
        const existsInReadmission = await CandidateUtility.CheckCandidateExistInReadmission(candidateDataUpdated);
        if (!existsInReadmission) {
          const scholarSeason = await ScholarSeasonModel.findById(candidateDataUpdated.scholar_season).lean();
          if (scholarSeason) {
            const start = moment(scholarSeason.from.date_utc, 'DD/MM/YYYY');
            const end = moment(scholarSeason.to.date_utc, 'DD/MM/YYYY');
            const now = moment().utc();
            if (now.isBetween(start, end, 'day', '[]')) {
              await CandidateModel.findByIdAndUpdate(candidateDataUpdated._id, {
                $set: { program_status: 'active' },
              });
            }
          }
          await CandidateUtility.createNextCandidateData(candidateDataUpdated);
        }

        await CandidateUtility.addRegisteredCandidateAsStudent({ candidate: candidateDataUpdated, lang });
        await CandidateUtility.send_REGISTRATION_N7(candidateDataUpdated, lang);
      }
    }
  }
} catch (err) {
  console.error(` Error processing signature auto-register for candidate_id: ${_id}`, err);
}

// *************** Update data to external systems
try {
  let updatedCandidateNew = await CandidateModel.findById(_id);

  await CandidateUtility.updateStudentBaseOnCandidate(updatedCandidateNew);

  // Prevent overwriting candidate_admission_status unexpectedly
  if (updatedCandidateNew.candidate_admission_status !== candidate_input.candidate_admission_status) {
    delete candidate_input.candidate_admission_status;
  }

  // *************** Clean payment_supports with null _id
  if (Array.isArray(candidate_input.payment_supports)) {
    candidate_input.payment_supports.forEach((support) => {
      if (!support._id) delete support._id;
    });
  }

  // *************** Sync billing payment method when changed
  if (
    oldCandidate.method_of_payment &&
    updatedCandidate.method_of_payment &&
    oldCandidate.method_of_payment !== updatedCandidate.method_of_payment &&
    updatedCandidate.intake_channel &&
    updatedCandidate.billing_id &&
    updatedCandidate.method_of_payment !== 'not_done'
  ) {
    await BillingModel.findByIdAndUpdate(updatedCandidate.billing_id, {
      $set: { payment_method: updatedCandidate.method_of_payment },
    });

    const actorId = userId || updatedCandidate.user_id;
    await BillingUtility.AddHistoryUpdateBilling(
      updatedCandidate.billing_id,
      'update_payment_method_down_payment',
      'UpdateCandidate',
      actorId
    );
  }

  // *************** Save final candidate updates
  updatedCandidateNew = await CandidateModel.findByIdAndUpdate(_id, { $set: candidate_input }, { new: true });

  // *************** Determine which step type was last updated
  let stepType = null;
  if (['done', 'pending'].includes(updatedCandidateNew.payment)) stepType = 'down_payment_mode';
  if (updatedCandidateNew.signature === 'done') stepType = 'step_with_signing_process';
  if (updatedCandidateNew.is_admited === 'done') stepType = 'summary';
  if (updatedCandidateNew.method_of_payment === 'done') stepType = 'modality_payment';
  if (updatedCandidateNew.presonal_information === 'done') stepType = 'question_and_field';
  if (updatedCandidateNew.connection === 'done') stepType = 'campus_validation';

  // *************** Update last_form_updated tracker
  if (stepType) {
    await CandidateModel.findByIdAndUpdate(_id, {
      $set: {
        last_form_updated: {
          step_type: stepType,
          date_updated: {
            date: moment.utc().format('DD/MM/YYYY'),
            time: moment.utc().format('HH:mm'),
          },
        },
      },
    });
  }
} catch (err) {
  console.error(` Error updating candidate final data for candidate_id: ${_id}`, err);
}


// *************** VALIDASI SPLIT PAYMENT & AUTO REGISTER
if (
  updatedCandidateNew.readmission_status !== 'readmission_table' &&
  updatedCandidateNew.signature === 'done' &&
  oldCandidate.signature !== 'done' &&
  typeOfFormation &&
  typeOfFormation.type_of_formation === 'classic'
) {
  if (updatedCandidateNew.payment === 'done') {
    updatedCandidateNew = await CandidateModel.findByIdAndUpdate(
      _id,
      {
        $set: {
          candidate_admission_status: 'registered',
          registered_at: {
            date: moment.utc().format('DD/MM/YYYY'),
            time: moment.utc().format('HH:mm'),
          },
        },
      },
      { new: true }
    );

    if (updatedCandidateNew?.candidate_admission_status === 'registered') {
      await CandidateUtility.addRegisteredCandidateAsStudent({ candidate: updatedCandidateNew });

      if (updatedCandidateNew.readmission_status !== 'readmission_table') {
        await CandidateUtility.send_REGISTRATION_N7(updatedCandidateNew);
      }

      if (!updatedCandidateNew.is_registration_recorded) {
        await GeneralDashboardAdmissionUtility.recordCandidateRegistered(updatedCandidateNew, userId);
      }

      await CandidateHistoryUtility.createNewCandidateHistory(
        updatedCandidateNew._id,
        userId,
        'update_candidate_campus',
        `Candidate ${updatedCandidateNew._id} registered`
      );
    }
  }
}

// *************** COMPARE FINANCE FIELD CHANGES
if (candidate_input?.finance && oldCandidate.finance !== candidate_input.finance) {
  await CandidateUtility.ValidateFinanceGenerated(updatedCandidateNew);

  switch (candidate_input.finance) {
    case 'family':
      await BillingUtility.ValidateAndSplitPaymentCandidateFinancialSupport(updatedCandidateNew);
      await MasterTransactionUtilities.GenerateStudentBalanceFI(_id);
      break;

    case 'my_self':
      await MasterTransactionUtilities.GenerateStudentBalanceFI(_id);
      break;

    case 'discount':
      if (typeOfFormation?.type_of_formation === 'classic') {
        await MasterTransactionUtilities.GenerateStudentBalanceFI(_id);
      }
      break;
  }
}

// *************** UPDATE FINANCIAL SUPPORT ON BILLING
if (updatedCandidateNew?.payment_supports?.length > 0) {
  await BillingUtility.updateFinancialSupportBilling(updatedCandidateNew);
}

// *************** GENERATE BALANCE IF REGISTERED
if (
  oldCandidate.candidate_admission_status !== 'registered' &&
  updatedCandidateNew.candidate_admission_status === 'registered'
) {
  if (updatedCandidateNew.admission_process_id) {
    await MasterTransactionUtilities.GenerateStudentBalance(
      _id,
      updatedCandidateNew.admission_process_id,
      true
    );
  } else {
    await MasterTransactionUtilities.GenerateStudentBalance(_id);
  }
}

// *************** SYNC STATUS TO OSCAR / HUBSPOT
if (
  candidate_input?.candidate_admission_status &&
  (
    oldCandidate.candidate_admission_status !== updatedCandidateNew.candidate_admission_status
  )
) {
  if (updatedCandidateNew.oscar_campus_id) {
    await CandidateUtility.changeCandidateStatusInOscarCampus(updatedCandidateNew);
  } else if (updatedCandidateNew.hubspot_deal_id && updatedCandidateNew.hubspot_contact_id) {
    await CandidateUtility.updateCandidateStatusFromHubspot(updatedCandidateNew);
  }
}

// *************** LOG STATUS HISTORY
if (
  candidate_input?.candidate_admission_status &&
  candidate_input.candidate_admission_status !== oldCandidate.candidate_admission_status
) {
  await CandidateModel.findByIdAndUpdate(_id, {
    $push: {
      status_update_histories: {
        type: is_from_admission_form ? 'platform' : 'user',
        userId: is_from_admission_form ? undefined : context.userId,
        previous_status: oldCandidate.candidate_admission_status,
        next_status: candidate_input.candidate_admission_status,
        datetime: {
          date: moment.utc().format('DD/MM/YYYY'),
          time: moment.utc().format('HH:mm'),
        },
      },
    },
  });
}

// *************** HANDLE MINOR STUDENT & EMANCIPATED DOC
if (is_minor_student) {
  const rejectEmancipatedDoc = await DocumentModel.findOne({
    _id: oldCandidate.emancipated_document_proof_id,
  }).sort({ _id: -1 });

  const isBecomingEmancipated =
    candidate_input.is_adult === false &&
    (!oldCandidate.is_adult || oldCandidate.is_adult === true) &&
    candidate_input.is_emancipated_minor === true &&
    (!oldCandidate.is_emancipated_minor || oldCandidate.is_emancipated_minor === false);

  const isResubmission =
    candidate_input.is_adult === oldCandidate.is_adult &&
    candidate_input.is_emancipated_minor === oldCandidate.is_emancipated_minor &&
    rejectEmancipatedDoc?.document_status === 'rejected';

  if (isBecomingEmancipated || isResubmission) {
    const emancipatedDoc = await DocumentModel.create({
      document_name: candidate_input.emancipated_document_proof_original_name || '',
      s3_file_name: candidate_input.emancipated_document_proof_name || '',
      type_of_document: 'emancipated_document_proof',
      document_generation_type: 'emancipated_document',
      document_status: 'validated',
      candidate_id: _id,
      program_id: updatedCandidateNew.intake_channel,
    });

    if (emancipatedDoc) {
      await CandidateModel.findByIdAndUpdate(updatedCandidateNew._id, {
        $set: { emancipated_document_proof_id: emancipatedDoc._id },
      });

      // Soft delete old rejected document
      if (rejectEmancipatedDoc) {
        await DocumentModel.findByIdAndUpdate(rejectEmancipatedDoc._id, {
          $set: { status: 'deleted' },
        });
      }
    }
  }
}

// *************** If candidate is not minor anymore and becomes handled by legal representative
if (is_minor_student === false) {
  const becameMinorHandledByLegalRep =
    candidate_input?.is_adult === false &&
    oldCandidate?.is_adult !== false &&
    candidate_input?.is_emancipated_minor === false &&
    oldCandidate?.is_emancipated_minor !== false;

  if (becameMinorHandledByLegalRep) {
    try {
      // send notification Minor_Student_N3
      await CandidateUtility.send_Minor_Student_N3(_id, lang);

      // set personal_information => legal_representative
      updatedCandidateNew = await CandidateModel.findByIdAndUpdate(
        updatedCandidateNew._id,
        { $set: { personal_information: 'legal_representative' } },
        { new: true }
      );

      // validate legal_representative exists on input
      const lr = candidate_input?.legal_representative;
      if (!lr) {
        throw new Error('legal_representative data is required');
      }

      // Prevent same email as candidate (case-insensitive, trimmed)
      const lrEmail = (lr.email || '').toString().trim().toLowerCase();
      const candidateEmail = (updatedCandidateNew?.email || '').toString().trim().toLowerCase();
      if (lrEmail && lrEmail === candidateEmail) {
        throw new Error('legal representative cannot have same email with candidate');
      }

      // derive civility default from parental_link
      const relations = ['father', 'grandfather', 'uncle'];
      const parentalLink = (lr.parental_link || '').toString().trim().toLowerCase();
      const civilityParentalLink = parentalLink === 'other' ? '' : relations.includes(parentalLink) ? 'MR' : 'MRS';

      // prepare legal representative payload with safe defaults
      const legalRepPayload = {
        unique_id: lr.unique_id || '',
        civility: lr.civility || civilityParentalLink || '',
        first_name: (lr.first_name || '').toString().trim(),
        last_name: (lr.last_name || '').toString().trim(),
        email: lrEmail || '',
        phone_number: (lr.phone_number || '').toString().trim(),
        parental_link: parentalLink || '',
        address: (lr.address || '').toString().trim(),
        postal_code: (lr.postal_code || '').toString().trim(),
        city: (lr.city || '').toString().trim(),
      };

      // update candidate with legal representative
      updatedCandidateNew = await CandidateModel.findByIdAndUpdate(
        updatedCandidateNew._id,
        { $set: { legal_representative: legalRepPayload } },
        { new: true }
      );
    } catch (err) {
      console.error(`⚠️ Failed to set legal representative for candidate ${_id}:`, err);
      throw err; // rethrow so caller knows update failed
    }
  }
}

// *************** Generate billing export controlling report (best-effort)
try {
  await BillingUtility.GenerateBillingExportControllingReport(updatedCandidateNew._id);
} catch (err) {
  console.error(`⚠️ Failed to generate billing export report for candidate ${updatedCandidateNew?._id}:`, err);
}

// Return fresh candidate document
return await CandidateModel.findById(updatedCandidateNew._id);


